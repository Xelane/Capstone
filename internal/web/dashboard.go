package web

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"
)

type NodeStatus struct {
	NodeID      string
	State       string // "Leader", "Follower", "Candidate"
	CurrentTerm int64
	AlivePeers  []string
	TotalPeers  int
}

type Dashboard struct {
	port           string
	statusFunc     func() NodeStatus
	commandHandler func(string) string // Handler for executing commands
	startTime      time.Time
}

func NewDashboard(port string, statusFunc func() NodeStatus, commandHandler func(string) string) *Dashboard {
	return &Dashboard{
		port:           port,
		statusFunc:     statusFunc,
		commandHandler: commandHandler,
		startTime:      time.Now(),
	}
}

func (d *Dashboard) Start() {
	http.HandleFunc("/", d.handleIndex)
	http.HandleFunc("/api/status", d.handleStatus)
	http.HandleFunc("/api/execute", d.handleExecute)

	fmt.Printf("📊 Dashboard at http://localhost:%s\n", d.port)
	go http.ListenAndServe(":"+d.port, nil)
}

func (d *Dashboard) handleIndex(w http.ResponseWriter, r *http.Request) {
	tmpl := `<!DOCTYPE html>
<html>
<head>
    <title>Distributed KV - {{.NodeID}}</title>
    <style>
        * { 
            margin: 0; 
            padding: 0; 
            box-sizing: border-box; 
        }
        
        :root {
            --primary: #1a1a1a;
            --secondary: #2d2d2d;
            --tertiary: #404040;
            --accent: #ffffff;
            --text: #ffffff;
            --text-secondary: #b0b0b0;
            --border: #333333;
            --success: #22c55e;
            --warning: #eab308;
            --error: #ef4444;
        }
        
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
            background: var(--primary);
            color: var(--text);
            min-height: 100vh;
            padding: 0;
        }
        
        .header {
            background: linear-gradient(135deg, var(--secondary) 0%, var(--tertiary) 100%);
            border-bottom: 1px solid var(--border);
            padding: 30px 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.3);
        }
        
        .header-content {
            max-width: 1200px;
            margin: 0 auto;
        }
        
        .header h1 {
            font-size: 2em;
            font-weight: 600;
            margin-bottom: 5px;
            letter-spacing: -0.5px;
        }
        
        .header-subtitle {
            color: var(--text-secondary);
            font-size: 0.95em;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .status-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: var(--success);
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.6; }
        }
        
        .main-container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 30px 20px;
        }
        
        .grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 30px;
            margin-bottom: 30px;
        }
        
        @media (max-width: 1024px) {
            .grid {
                grid-template-columns: 1fr;
            }
        }
        
        .card {
            background: var(--secondary);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.2);
            transition: border-color 0.3s ease, box-shadow 0.3s ease;
        }
        
        .card:hover {
            border-color: var(--tertiary);
            box-shadow: 0 8px 16px rgba(0,0,0,0.3);
        }
        
        .card h2 {
            font-size: 1.3em;
            font-weight: 600;
            margin-bottom: 20px;
            color: var(--accent);
            padding-bottom: 15px;
            border-bottom: 1px solid var(--border);
        }
        
        .status-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
        }
        
        .status-item {
            background: var(--primary);
            padding: 15px;
            border-radius: 8px;
            border: 1px solid var(--border);
            text-align: center;
        }
        
        .status-label {
            color: var(--text-secondary);
            font-size: 0.85em;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 500;
        }
        
        .status-value {
            font-size: 1.8em;
            font-weight: 700;
            color: var(--accent);
        }
        
        .badge {
            display: inline-block;
            padding: 6px 16px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .badge.leader {
            background: rgba(34, 197, 94, 0.2);
            color: var(--success);
            border: 1px solid var(--success);
        }
        
        .badge.follower {
            background: rgba(176, 176, 176, 0.2);
            color: var(--text-secondary);
            border: 1px solid var(--tertiary);
        }
        
        .badge.candidate {
            background: rgba(234, 179, 8, 0.2);
            color: var(--warning);
            border: 1px solid var(--warning);
        }
        
        .peers {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 15px;
        }
        
        .peer {
            background: var(--primary);
            color: var(--success);
            padding: 8px 14px;
            border-radius: 6px;
            font-size: 0.85em;
            border: 1px solid var(--success);
            font-weight: 500;
        }
        
        .peer.offline {
            color: var(--error);
            border-color: var(--error);
        }
        
        .client-section {
            grid-column: 1 / -1;
        }
        
        .command-input-group {
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
        }
        
        .command-input {
            flex: 1;
            background: var(--primary);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 12px 16px;
            color: var(--text);
            font-family: 'Monaco', 'Courier New', monospace;
            font-size: 0.95em;
            transition: border-color 0.3s ease;
        }
        
        .command-input:focus {
            outline: none;
            border-color: var(--accent);
            box-shadow: 0 0 0 3px rgba(255, 255, 255, 0.1);
        }
        
        .command-button {
            background: var(--accent);
            color: var(--primary);
            border: none;
            padding: 12px 24px;
            border-radius: 8px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            font-size: 0.95em;
        }
        
        .command-button:hover {
            background: var(--text-secondary);
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.2);
        }
        
        .command-button:active {
            transform: translateY(0);
        }
        
        .result-display {
            background: var(--primary);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 15px;
            font-family: 'Monaco', 'Courier New', monospace;
            font-size: 0.9em;
            min-height: 100px;
            max-height: 300px;
            overflow-y: auto;
            line-height: 1.6;
        }
        
        .result-line {
            margin: 5px 0;
            padding: 5px;
        }
        
        .result-line.input {
            color: var(--accent);
        }
        
        .result-line.output {
            color: var(--text-secondary);
        }
        
        .result-line.success {
            color: var(--success);
        }
        
        .result-line.error {
            color: var(--error);
        }
        
        .help-text {
            color: var(--text-secondary);
            font-size: 0.85em;
            margin-top: 10px;
            font-family: 'Monaco', 'Courier New', monospace;
        }
        
        .help-text code {
            background: var(--primary);
            padding: 2px 6px;
            border-radius: 4px;
            color: var(--accent);
        }
        
        .peers-list {
            display: grid;
            grid-template-columns: 1fr;
            gap: 10px;
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="header-content">
            <h1>Distributed Database</h1>
            <div class="header-subtitle">
                <span class="status-dot"></span>
                <span>Node <strong id="nodeId">-</strong> | <span id="stateLabel" style="color: var(--success);">ACTIVE</span></span>
            </div>
        </div>
    </div>
    
    <div class="main-container">
        <div class="grid">
            <!-- Node Status Card -->
            <div class="card">
                <h2>Node Information</h2>
                <div class="status-grid">
                    <div class="status-item">
                        <div class="status-label">Current State</div>
                        <span id="state" class="badge follower">-</span>
                    </div>
                    <div class="status-item">
                        <div class="status-label">Consensus Term</div>
                        <div id="term" class="status-value">-</div>
                    </div>
                    <div class="status-item">
                        <div class="status-label">Connected Peers</div>
                        <div id="peerCount" class="status-value">-</div>
                    </div>
                    <div class="status-item">
                        <div class="status-label">Uptime</div>
                        <div id="uptime" class="status-value">-</div>
                    </div>
                </div>
            </div>
            
            <!-- Cluster Peers Card -->
            <div class="card">
                <h2>Cluster Peers</h2>
                <div id="peers" class="peers-list"></div>
            </div>
        </div>
        
        <!-- Client Interface Card -->
        <div class="card client-section">
            <h2>Client Terminal</h2>
            <div class="command-input-group">
                <input 
                    type="text" 
                    id="commandInput" 
                    class="command-input" 
                    placeholder="Enter command: PUT key value | GET key | DELETE key"
                    autocomplete="off"
                />
                <button class="command-button" onclick="executeCommand()">Execute</button>
            </div>
            <div class="result-display" id="resultDisplay">
                <div class="result-line output">Ready for commands...</div>
            </div>
            <div class="help-text">
                Examples: 
                <code>PUT username Alice</code> | 
                <code>GET username</code> | 
                <code>DELETE username</code>
            </div>
        </div>
    </div>

    <script>
        const resultDisplay = document.getElementById('resultDisplay');
        const commandInput = document.getElementById('commandInput');
        let resultLines = [];
        
        // Allow Enter key to submit command
        commandInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                executeCommand();
            }
        });
        
        function addResultLine(text, type = 'output') {
            resultLines.push({ text, type });
            if (resultLines.length > 50) {
                resultLines.shift();
            }
            updateResultDisplay();
        }
        
        function updateResultDisplay() {
            resultDisplay.innerHTML = resultLines.map(line => 
                '<div class="result-line ' + line.type + '">' + escapeHtml(line.text) + '</div>'
            ).join('');
            resultDisplay.scrollTop = resultDisplay.scrollHeight;
        }
        
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        function executeCommand() {
            const command = commandInput.value.trim();
            
            if (!command) return;
            
            addResultLine('> ' + command, 'input');
            commandInput.value = '';
            
            fetch('/api/execute', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ command: command })
            })
            .then(r => r.json())
            .then(data => {
                if (data.error) {
                    addResultLine('ERROR: ' + data.error, 'error');
                } else {
                    const result = data.result || data.message || JSON.stringify(data);
                    if (result.startsWith('ERROR')) {
                        addResultLine(result, 'error');
                    } else if (result === 'OK' || result.startsWith('✓')) {
                        addResultLine(result, 'success');
                    } else {
                        addResultLine(result, 'output');
                    }
                }
            })
            .catch(err => {
                addResultLine('ERROR: ' + err.message, 'error');
            });
        }
        
        function updateDashboard() {
            fetch('/api/status')
                .then(r => r.json())
                .then(data => {
                    document.getElementById('nodeId').textContent = data.NodeID;
                    
                    const stateBadge = document.getElementById('state');
                    stateBadge.textContent = data.State.toUpperCase();
                    stateBadge.className = 'badge ' + data.State.toLowerCase();
                    
                    const stateLabel = document.getElementById('stateLabel');
                    stateLabel.textContent = data.State.toUpperCase();
                    stateLabel.style.color = data.State === 'Leader' ? 'var(--success)' : 'var(--text-secondary)';
                    
                    document.getElementById('term').textContent = data.CurrentTerm;
                    document.getElementById('peerCount').textContent = 
                        data.AlivePeers.length + ' / ' + data.TotalPeers;
                    document.getElementById('uptime').textContent = data.Uptime;
                    
                    const peersDiv = document.getElementById('peers');
                    if (data.AlivePeers.length === 0) {
                        peersDiv.innerHTML = '<div class="result-line output">No peers connected</div>';
                    } else {
                        peersDiv.innerHTML = data.AlivePeers.map(p => 
                            '<div class="peer">✓ ' + escapeHtml(p) + '</div>'
                        ).join('');
                    }
                });
        }
        
        setInterval(updateDashboard, 2000);
        updateDashboard();
    </script>
</body>
</html>`

	status := d.statusFunc()
	t := template.Must(template.New("index").Parse(tmpl))
	t.Execute(w, status)
}

func (d *Dashboard) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := d.statusFunc()

	// Add uptime
	uptime := time.Since(d.startTime).Round(time.Second).String()

	response := map[string]interface{}{
		"NodeID":      status.NodeID,
		"State":       status.State,
		"CurrentTerm": status.CurrentTerm,
		"AlivePeers":  status.AlivePeers,
		"TotalPeers":  status.TotalPeers,
		"Uptime":      uptime,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (d *Dashboard) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"error": "Method not allowed"})
		return
	}

	var req struct {
		Command string `json:"command"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"error": "Invalid request"})
		return
	}

	command := strings.TrimSpace(req.Command)
	if command == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"error": "Empty command"})
		return
	}

	// Execute the command using the handler
	result := ""
	if d.commandHandler != nil {
		result = d.commandHandler(command)
	} else {
		result = "ERROR: Command handler not configured"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"result": result,
	})
}
