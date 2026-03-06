# Antigravity Assistant Constraints

## 🚨 SYSTEM CRITICAL: Port Management
- **Sacred Port 80**: This port belongs EXCLUSIVELY to the Colosal ERP Production Server (Waitress).
- **No Listeners on 80**: Antigravity must NEVER start any internal tool, dev server, or browser debug listener on port 80.
- **Default Testing Port**: If Antigravity needs to spin up a temporary listener for diagnostics or testing, it MUST use port **5000** or higher.
- **Browser Subagents**: Automated browser tasks must be initialized with remote-debugging-port DISABLED or set to a non-conflicting port (e.g., 9222).

## 🛡️ Rationale
To ensure the production environment remains stable and accessible to end-users without technical interference from the AI's background tasks.
