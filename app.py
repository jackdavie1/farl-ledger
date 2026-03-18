import os
import hashlib
import json
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from sqlalchemy import inspect

app = Flask(__name__)
CORS(app)

basedir = os.path.abspath(os.path.dirname(__file__))

database_url = os.getenv("DATABASE_URL")
if database_url and database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = database_url or ("sqlite:///" + os.path.join(basedir, "ledger.db"))
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

print("FARL ledger booting...", flush=True)
print(f"PORT={os.getenv('PORT', 'unset')}", flush=True)
print(f"DATABASE_URL present={bool(os.getenv('DATABASE_URL'))}", flush=True)
print(f"SQLALCHEMY_DATABASE_URI={app.config['SQLALCHEMY_DATABASE_URI']}", flush=True)

db = SQLAlchemy(app)


class Event(db.Model):
    __tablename__ = "events"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    timestamp = db.Column(db.String, nullable=False)
    entry_type = db.Column(db.String, nullable=False)
    kind = db.Column(db.String, nullable=True)
    source = db.Column(db.String, nullable=True)
    chapter = db.Column(db.String, nullable=True)
    payload = db.Column(db.Text, nullable=False)
    prev_hash = db.Column(db.String, nullable=False)
    entry_hash = db.Column(db.String, nullable=False, unique=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    def payload_obj(self):
        try:
            return json.loads(self.payload)
        except Exception:
            return {"raw_payload": self.payload}

    def to_dict(self, include_payload=True):
        data = {
            "id": self.id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "timestamp": self.timestamp,
            "entry_type": self.entry_type,
            "kind": self.kind,
            "source": self.source,
            "chapter": self.chapter,
            "entry_hash": self.entry_hash,
            "prev_hash": self.prev_hash,
        }
        if include_payload:
            data["payload"] = self.payload_obj()
        return data


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def canonical_json(data):
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def calculate_hash(timestamp, entry_type, payload_str, prev_hash):
    block_string = f"{timestamp}{entry_type}{payload_str}{prev_hash}".encode()
    return hashlib.sha256(block_string).hexdigest()


def ensure_optional_columns():
    inspector = inspect(db.engine)
    columns = {col["name"] for col in inspector.get_columns("events")}

    with db.engine.begin() as conn:
        if "kind" not in columns:
            conn.exec_driver_sql("ALTER TABLE events ADD COLUMN kind VARCHAR")
        if "source" not in columns:
            conn.exec_driver_sql("ALTER TABLE events ADD COLUMN source VARCHAR")
        if "chapter" not in columns:
            conn.exec_driver_sql("ALTER TABLE events ADD COLUMN chapter VARCHAR")


def envelope(ok=True, data=None, error=None, status=200):
    return jsonify({"ok": ok, "data": data, "error": error}), status


@app.route("/health", methods=["GET"])
def health_check():
    return envelope(
        ok=True,
        data={"status": "healthy", "node": "FARL_LEDGER"},
        error=None,
    )


@app.route("/status", methods=["GET"])
def status_check():
    latest = Event.query.order_by(Event.id.desc()).first()
    return envelope(
        ok=True,
        data={
            "node": "FARL_LEDGER",
            "latest_entry_id": latest.id if latest else None,
            "latest_entry_type": latest.entry_type if latest else None,
            "latest_timestamp": latest.timestamp if latest else None,
            "chain_length": Event.query.count(),
        },
        error=None,
    )


@app.route("/log", methods=["POST"])
def log_event():
    data = request.get_json(silent=True)
    if not data or "entry_type" not in data or "payload" not in data:
        return envelope(
            ok=False,
            data=None,
            error="Invalid payload: requires entry_type and payload",
            status=400,
        )

    if not isinstance(data["payload"], dict):
        return envelope(
            ok=False,
            data=None,
            error="payload must be a JSON object",
            status=400,
        )

    server_now = utc_now_iso()
    last_entry = Event.query.order_by(Event.id.desc()).first()
    prev_hash = last_entry.entry_hash if last_entry else "0" * 64

    payload_str = canonical_json(data["payload"])
    new_hash = calculate_hash(server_now, data["entry_type"], payload_str, prev_hash)

    new_event = Event(
        timestamp=server_now,
        entry_type=data["entry_type"],
        kind=data.get("kind") or data["payload"].get("kind"),
        source=data.get("source") or data["payload"].get("source"),
        chapter=data.get("chapter") or data["payload"].get("chapter"),
        payload=payload_str,
        prev_hash=prev_hash,
        entry_hash=new_hash,
    )

    db.session.add(new_event)
    db.session.commit()

    return envelope(
        ok=True,
        data={
            "status": "recorded",
            "entry_id": new_event.id,
            "entry_hash": new_hash,
            "timestamp": server_now,
            "entry": new_event.to_dict(include_payload=True),
        },
        error=None,
        status=201,
    )


@app.route("/latest", methods=["GET"])
def get_latest_legacy():
    last_entry = Event.query.order_by(Event.id.desc()).first()
    if not last_entry:
        return jsonify({}), 200
    return jsonify(last_entry.to_dict(include_payload=True)), 200


@app.route("/entries/latest", methods=["GET"])
def get_latest_entry():
    last_entry = Event.query.order_by(Event.id.desc()).first()
    return envelope(
        ok=True,
        data=last_entry.to_dict(include_payload=True) if last_entry else None,
        error=None,
    )


@app.route("/entries/<int:entry_id>", methods=["GET"])
def get_entry_by_id(entry_id):
    entry = Event.query.filter_by(id=entry_id).first()
    if not entry:
        return envelope(ok=False, data=None, error="Entry not found", status=404)
    return envelope(ok=True, data=entry.to_dict(include_payload=True), error=None)


@app.route("/entries/by-hash/<string:entry_hash>", methods=["GET"])
def get_entry_by_hash(entry_hash):
    entry = Event.query.filter_by(entry_hash=entry_hash).first()
    if not entry:
        return envelope(ok=False, data=None, error="Entry not found", status=404)
    return envelope(ok=True, data=entry.to_dict(include_payload=True), error=None)


@app.route("/entries", methods=["GET"])
def get_entries():
    limit = request.args.get("limit", default=200, type=int)
    offset = request.args.get("offset", default=0, type=int)
    before_id = request.args.get("before_id", type=int)
    after_id = request.args.get("after_id", type=int)
    order = request.args.get("order", default="asc", type=str).lower()
    entry_type = request.args.get("entry_type", type=str)
    kind = request.args.get("kind", type=str)
    source = request.args.get("source", type=str)
    chapter = request.args.get("chapter", type=str)
    from_ts = request.args.get("from_ts", type=str)
    to_ts = request.args.get("to_ts", type=str)
    include_payload = request.args.get("include_payload", default="true", type=str).lower() != "false"

    limit = max(1, min(limit, 1000))

    query = Event.query

    if before_id is not None:
        query = query.filter(Event.id < before_id)
    if after_id is not None:
        query = query.filter(Event.id > after_id)
    if entry_type:
        query = query.filter(Event.entry_type == entry_type)
    if kind:
        query = query.filter(Event.kind == kind)
    if source:
        query = query.filter(Event.source == source)
    if chapter:
        query = query.filter(Event.chapter == chapter)
    if from_ts:
        query = query.filter(Event.timestamp >= from_ts)
    if to_ts:
        query = query.filter(Event.timestamp <= to_ts)

    if order == "desc":
        query = query.order_by(Event.id.desc())
    else:
        query = query.order_by(Event.id.asc())

    total_matching = query.count()
    rows = query.offset(offset).limit(limit).all()

    next_offset = offset + len(rows) if (offset + len(rows)) < total_matching else None
    next_before_id = rows[-1].id if rows else None

    return envelope(
        ok=True,
        data={
            "entries": [row.to_dict(include_payload=include_payload) for row in rows],
            "count": len(rows),
            "total_matching": total_matching,
            "next_offset": next_offset,
            "next_before_id": next_before_id,
            "has_more": next_offset is not None,
        },
        error=None,
    )


@app.route("/ledger/stats", methods=["GET"])
def ledger_stats():
    from_ts = request.args.get("from_ts", type=str)
    to_ts = request.args.get("to_ts", type=str)

    query = Event.query
    if from_ts:
        query = query.filter(Event.timestamp >= from_ts)
    if to_ts:
        query = query.filter(Event.timestamp <= to_ts)

    rows = query.order_by(Event.id.asc()).all()

    entry_type_counts = {}
    kind_counts = {}
    source_counts = {}
    chapter_counts = {}

    for row in rows:
        entry_type_counts[row.entry_type] = entry_type_counts.get(row.entry_type, 0) + 1
        if row.kind:
            kind_counts[row.kind] = kind_counts.get(row.kind, 0) + 1
        if row.source:
            source_counts[row.source] = source_counts.get(row.source, 0) + 1
        if row.chapter:
            chapter_counts[row.chapter] = chapter_counts.get(row.chapter, 0) + 1

    return envelope(
        ok=True,
        data={
            "total_entries": len(rows),
            "entry_type_counts": entry_type_counts,
            "kind_counts": kind_counts,
            "source_counts": source_counts,
            "chapter_counts": chapter_counts,
            "from_ts": from_ts,
            "to_ts": to_ts,
        },
        error=None,
    )


@app.route("/verify", methods=["GET"])
def verify_chain():
    all_events = Event.query.order_by(Event.id.asc()).all()
    is_valid = True
    expected_prev_hash = "0" * 64
    failed_at_id = None

    for event in all_events:
        actual_hash = calculate_hash(event.timestamp, event.entry_type, event.payload, event.prev_hash)
        if actual_hash != event.entry_hash or event.prev_hash != expected_prev_hash:
            is_valid = False
            failed_at_id = event.id
            break
        expected_prev_hash = event.entry_hash

    return envelope(
        ok=is_valid,
        data={
            "integrity_check": "passed" if is_valid else "failed",
            "chain_length": len(all_events),
            "failed_at_id": failed_at_id,
        },
        error=None if is_valid else "Chain verification failed",
        status=200 if is_valid else 500,
    )


with app.app_context():
    db.create_all()
    ensure_optional_columns()
    print("Database tables ready.", flush=True)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
