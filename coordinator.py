"""Учебный координатор: baseline, init, run, pause, resume, show."""

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import io
from itertools import combinations
import json
import logging
import os
from pathlib import Path
import re
import signal
import sys
import time

from kazoo.client import KazooClient
from kazoo.exceptions import (
    BadVersionError,
    ConnectionLoss,
    NodeExistsError,
    RolledBackError,
    SessionExpiredError,
)
from source_tools import is_variant, parse_skills

ROOT = Path(__file__).resolve().parent
HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
LOG = logging.getLogger("coordinator")


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def event(kind, **fields):
    LOG.info(
        encode(
            {
                "event": kind,
                "time": datetime.now(timezone.utc).isoformat(),
                "mono_ns": time.monotonic_ns(),
                "pid": os.getpid(),
                **fields,
            }
        ).decode()
    )


def commit(transaction):
    results = transaction.commit()
    errors = [value for value in results if isinstance(value, Exception)]
    if errors:
        raise next(
            (error for error in errors if not isinstance(error, RolledBackError)),
            errors[0],
        )
    return results


def wait_connected(zk):
    deadline = time.monotonic() + 15
    while not zk.connected:
        if time.monotonic() > deadline:
            raise TimeoutError("Нет соединения с ZooKeeper")
        time.sleep(0.1)


def read_input(path):
    csv.field_size_limit(10_000_000)
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def add_chunk(counts, rows):
    selected = 0
    for row in rows:
        if is_variant(row):
            skills = sorted(
                {s.strip().lower() for s in parse_skills(row["key_skills"]) if s.strip()}
            )
            counts.update(combinations(skills, 2))
            selected += 1
    return selected


def csv_bytes(counts):
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(["skills", "n"])
    for key, n in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        writer.writerow([json.dumps(key, ensure_ascii=False), n])
    return stream.getvalue().encode()


def verify_result(counts, total, selected, baseline):
    blob = csv_bytes(counts)
    if blob != baseline.read_bytes():
        raise AssertionError("CSV не совпал с эталоном ДЗ № 3")
    return blob, {
        "source_rows": total,
        "selected_rows": selected,
        "unique_pairs": len(counts),
        "pair_contributions": sum(counts.values()),
        "sha256": hashlib.sha256(blob).hexdigest(),
        "equal_to_hw3": True,
    }


def claim(zk, root, name):
    data, stat = zk.get(root + "/epoch")
    epoch = int(data) + 1
    transaction = zk.transaction()
    transaction.check(root + "/epoch", stat.version)
    transaction.check(root + "/result", 0)
    transaction.set_data(root + "/epoch", str(epoch).encode())
    transaction.create(
        root + "/leader", encode({"worker": name, "pid": os.getpid()}), ephemeral=True
    )
    try:
        commit(transaction)
        assert epoch == stat.version + 1
        return epoch
    except (BadVersionError, NodeExistsError):
        return None


def publish(hosts, root, epoch, pointer, fencing):
    """Получатель - узел result в ZooKeeper; проверка и запись атомарны.

    CSV неизменяемый и адресуется хешем. Защищается его публикация через указатель.
    Для внешней БД ту же проверку нужно выполнять атомарно в самой БД.
    """
    # Новая сессия позволяет отправить запрос после SIGCONT, но токен остаётся старым.
    zk = KazooClient(hosts=hosts, timeout=10.0)
    try:
        zk.start(timeout=15)
        if not fencing:
            return zk.set(root + "/result", encode(pointer)).version
        transaction = zk.transaction()
        transaction.check(root + "/epoch", epoch)
        transaction.set_data(root + "/result", encode(pointer))
        return commit(transaction)[1].version
    finally:
        zk.stop()
        zk.close()


def worker(zk, root, args):
    logfile = ROOT / "logs" / f"{args.run}-{args.name}.log"
    LOG.setLevel(logging.INFO)
    for handler in (
        logging.FileHandler(logfile, mode="x", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ):
        handler.setFormatter(logging.Formatter("%(message)s"))
        LOG.addHandler(handler)
    (ROOT / "logs" / f"{args.run}-{args.name}.pid").write_text(str(os.getpid()))
    event(
        "session",
        worker=args.name,
        requested_timeout_ms=10000,
        negotiated_timeout_ms=zk._session_timeout,
    )
    if zk._session_timeout != 10000:
        raise RuntimeError("Согласованный тайм-аут сессии должен быть 10000 мс")
    config = json.loads(zk.get(root)[0])
    if hashlib.sha256(args.input.read_bytes()).hexdigest() != config["input_sha256"]:
        raise ValueError("Исходный датасет изменился после init")
    rows = read_input(args.input)
    epoch = None
    while epoch is None:
        zk.sync(root + "/result")
        if json.loads(zk.get(root + "/result")[0]):
            event("already_completed", worker=args.name)
            return
        if not zk.exists(root + "/leader"):
            epoch = claim(zk, root, args.name)
        if epoch is None:
            time.sleep(0.1)
    event("became_leader", worker=args.name, epoch=epoch)
    state = json.loads(zk.get(root + "/checkpoint")[0])
    counts = Counter({tuple(key): n for key, n in state.get("counts", [])})
    next_row, selected = state.get("next_row", 0), state.get("selected_rows", 0)
    event("restored", worker=args.name, epoch=epoch, next_row=next_row)
    can_checkpoint = True
    while next_row < len(rows):
        chunk = rows[next_row : next_row + 1000]
        selected += add_chunk(counts, chunk)
        next_row += len(chunk)
        if can_checkpoint:
            state = {
                "next_row": next_row,
                "selected_rows": selected,
                "counts": [[list(key), n] for key, n in sorted(counts.items())],
            }
            payload = encode(state)
            if len(payload) >= 900_000:
                raise ValueError("Учебный чекпоинт слишком велик для znode")
            wait_connected(zk)
            transaction = zk.transaction()
            transaction.check(root + "/epoch", epoch)
            transaction.set_data(root + "/checkpoint", payload)
            try:
                commit(transaction)
                event(
                    "checkpoint",
                    worker=args.name,
                    epoch=epoch,
                    next_row=next_row,
                    checkpoint_bytes=len(payload),
                )
            except (BadVersionError, ConnectionLoss, SessionExpiredError) as exc:
                can_checkpoint = False
                event("checkpoint_rejected", worker=args.name, error=type(exc).__name__)
        time.sleep(0.2)
    blob, numbers = verify_result(counts, len(rows), selected, args.baseline)
    event("computed", worker=args.name, epoch=epoch, **numbers)
    folder = ROOT / "results" / args.run
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / (numbers["sha256"] + ".csv")
    temporary = folder / f"{args.name}.tmp"
    temporary.write_bytes(blob)
    os.replace(temporary, target)
    pointer = {
        "worker": args.name,
        "epoch": epoch,
        "csv": str(target.relative_to(ROOT)),
        **numbers,
    }
    # Старый процесс намеренно продолжает локальный расчёт после SIGCONT.
    # Это демонстрирует устаревший запрос; отмена задачи не заменяет fencing.
    try:
        version = publish(args.hosts, root, epoch, pointer, config["fencing"])
        event("записал результат", worker=args.name, epoch=epoch, pointer_version=version)
    except BadVersionError:
        event(
            "publication_rejected",
            worker=args.name,
            epoch=epoch,
            error="BadVersionError",
        )


def control(zk, root, args):
    path = ROOT / "logs" / f"{args.run}-pause.json"
    pid = int((ROOT / "logs" / f"{args.run}-{args.name}.pid").read_text())
    if args.action == "pause":
        leader = json.loads(zk.get(root + "/leader")[0])
        if leader != {"worker": args.name, "pid": pid}:
            raise RuntimeError("Указанный процесс не является лидером")
        info = {"worker": args.name, "pid": pid, "paused_ns": time.monotonic_ns()}
        os.kill(pid, signal.SIGSTOP)
        path.write_bytes(encode(info))
        print("SIGSTOP:", json.dumps(info, ensure_ascii=False))
    else:
        info = json.loads(path.read_bytes())
        pointer = json.loads(zk.get(root + "/result")[0])
        if not pointer or pointer["worker"] == args.name:
            raise RuntimeError("Сначала дождитесь публикации запасного координатора")
        if info["worker"] != args.name or info["pid"] != pid:
            raise ValueError("PID не совпадает с приостановленным процессом")
        elapsed = (time.monotonic_ns() - info["paused_ns"]) / 1e9
        if elapsed < 40:
            raise RuntimeError(f"Пауза длится {elapsed:.1f} с; дождитесь 40 с")
        os.kill(pid, signal.SIGCONT)
        info["resumed_ns"] = time.monotonic_ns()
        path.write_bytes(encode(info))
        print(f"SIGCONT: worker={args.name}, pause_seconds={elapsed:.3f}")


def show(zk, root, args):
    data, stat = zk.get(root + "/result")
    result = {
        "run": args.run,
        "fencing": json.loads(zk.get(root)[0])["fencing"],
        "pointer_version": stat.version,
        "pointer": json.loads(data),
    }
    if result["pointer"]:
        result["csv_equal_to_hw3"] = (
            ROOT / result["pointer"]["csv"]
        ).read_bytes() == args.baseline.read_bytes()
    pause_file = ROOT / "logs" / f"{args.run}-pause.json"
    if pause_file.exists():
        pause = json.loads(pause_file.read_bytes())
        events = []
        for path in sorted((ROOT / "logs").glob(f"{args.run}-*.log")):
            events.extend(json.loads(line) for line in path.read_text().splitlines())
        leaders = [
            item
            for item in events
            if item["event"] == "became_leader"
            and item["worker"] != pause["worker"]
            and item["mono_ns"] > pause["paused_ns"]
        ]
        if leaders:
            result["failover_seconds"] = (
                min(item["mono_ns"] for item in leaders) - pause["paused_ns"]
            ) / 1e9
        result["accepted_writes"] = sum(item["event"] == "записал результат" for item in events)
        result["stale_publication_rejected"] = any(
            item["event"] == "publication_rejected" for item in events
        )
        if "resumed_ns" in pause:
            result["pause_seconds"] = (pause["resumed_ns"] - pause["paused_ns"]) / 1e9
    print(json.dumps(result, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["baseline", "init", "run", "pause", "resume", "show"])
    parser.add_argument("run", nargs="?", default="check")
    parser.add_argument("name", nargs="?", choices=["A", "B"])
    parser.add_argument("--hosts", default=HOSTS)
    parser.add_argument(
        "--input",
        type=Path,
        default=ROOT.parent / "hh-cpp-mapreduce-lab/data/hh_it_vacancies.csv",
    )
    parser.add_argument("--baseline", type=Path, default=ROOT / "counts_k2.csv")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.run):
        parser.error("Имя опыта: только латинские буквы, цифры, _ и -")
    if args.action in ("run", "pause", "resume") and args.name is None:
        parser.error("Укажите имя координатора: A или B")
    (ROOT / "logs").mkdir(exist_ok=True)
    if args.action == "baseline":
        rows = read_input(args.input)
        counts = Counter()
        selected = add_chunk(counts, rows)
        _, numbers = verify_result(counts, len(rows), selected, args.baseline)
        print(json.dumps(numbers, ensure_ascii=False, indent=2))
        return
    root = "/zk-homework/" + args.run
    zk = KazooClient(hosts=args.hosts, timeout=10.0)
    try:
        zk.start(timeout=15)
        if args.action == "init":
            fencing = int(os.environ.get("FENCING", "0"))
            if fencing not in (0, 1):
                raise ValueError("FENCING должен быть 0 или 1")
            config = {
                "input_sha256": hashlib.sha256(args.input.read_bytes()).hexdigest(),
                "fencing": fencing,
            }
            zk.create(root, encode(config), makepath=True)
            for name, value in (
                ("epoch", b"0"),
                ("checkpoint", b"{}"),
                ("result", b"{}"),
            ):
                zk.create(root + "/" + name, value)
            print(f"Создан опыт {root}; FENCING={fencing}; версия указателя=0")
        elif args.action == "run":
            worker(zk, root, args)
        elif args.action in ("pause", "resume"):
            control(zk, root, args)
        else:
            show(zk, root, args)
    finally:
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()
