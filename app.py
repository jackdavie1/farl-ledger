import os
import hashlib
import json
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Database Configuration
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'ledger.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# --- MODELS ---

class Event(db.Model):
    __tablename__ = 'events'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    timestamp = db.Column(db.String, nullable=False)
    entry_type = db.Column(db.String, nullable=False)
    payload = db.Column(db.Text, nullable=False)
    prev_hash = db.Column(db.String, nullable=False)
    entry_hash = db.Column(db.String, nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    def to_dict(self):
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "entry_type": self.entry_type,
            "payload": json.loads(self.payload),
            "prev_hash": self.prev_hash,
            "entry_hash": self.entry_hash,
            "created_at": self.created_at.isoformat()
        }

# --- UTILS ---

def calculate_hash(timestamp, entry_type, payload, prev_hash):
    """Calculates SHA-256 hash of the entry components."""
    block_string = f"{timestamp}{entry_type}{payload}{prev_hash}".encode()
    return hashlib.sha256(block_string).hexdigest()

# --- ENDPOINTS ---

@app.route('/health', methods=['GET'])
def health_check():
    """Health check for self-healing infrastructure."""
    return jsonify({"status": "healthy", "node": "Gemini_Build_01"}), 200

@app.route('/log', methods=['POST'])
def log_event():
    """Appends a new immutable entry to the ledger."""
    data = request.get_json()
    if not data or 'entry_type' not in data or 'payload' not in data:
        return jsonify({"error": "Invalid payload"}), 400

    # Enforcement: Server-side UTC only
    server_now = datetime.now(timezone.utc).isoformat()
    
    # Get previous hash for chaining
    last_entry = Event.query.order_by(Event.id.desc()).first()
    prev_hash = last_entry.entry_hash if last_entry else "0" * 64

    # Serialize payload
    payload_str = json.dumps(data['payload'])
    
    # Calculate hash for integrity
    new_hash = calculate_hash(server_now, data['entry_type'], payload_str, prev_hash)

    # Commit to DB (Append-only: No UPDATE/DELETE defined)
    new_event = Event(
        timestamp=server_now,
        entry_type=data['entry_type'],
        payload=payload_str,
        prev_hash=prev_hash,
        entry_hash=new_hash
    )
    
    db.session.add(new_event)
    db.session.commit()

    return jsonify({
        "status": "recorded",
        "entry_id": new_event.id,
        "entry_hash": new_hash
    }), 201

@app.route('/entries', methods=['GET'])
def get_entries():
    """Returns paginated entries from the ledger."""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    events_query = Event.query.order_by(Event.id.asc()).paginate(page=page, per_page=per_page)
    
    return jsonify({
        "entries": [e.to_dict() for e in events_query.items],
        "total": events_query.total,
        "pages": events_query.pages,
        "current_page": page
    }), 200

@app.route('/verify', methods=['GET'])
def verify_chain():
    """Verifies the integrity of the entire hash chain."""
    all_events = Event.query.order_by(Event.id.asc()).all()
    is_valid = True
    expected_prev_hash = "0" * 64
    
    for e in all_events:
        actual_hash = calculate_hash(e.timestamp, e.entry_type, e.payload, e.prev_hash)
        if actual_hash != e.entry_hash or e.prev_hash != expected_prev_hash:
            is_valid = False
            break
        expected_prev_hash = e.entry_hash

    return jsonify({
        "integrity_check": "passed" if is_valid else "failed",
        "chain_length": len(all_events)
    }), 200 if is_valid else 500

with app.app_context():
    db.create_all()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

