package web

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
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
	port       string
	statusFunc func() NodeStatus
	startTime  time.Time
}

func NewDashboard(port string, statusFunc func() NodeStatus) *Dashboard {
	return &Dashboard{
		port:       port,
		statusFunc: statusFunc,
		startTime:  time.Now(),
	}
}

func (d *Dashboard) Start() {
	http.HandleFunc("/", d.handleIndex)
	http.HandleFunc("/api/status", d.handleStatus)

	fmt.Printf("📊 Dashboard at http://localhost:%s\n", d.port)
	go http.ListenAndServe(":"+d.port, nil)
}

func (d *Dashboard) handleIndex(w http.ResponseWriter, r *http.Request) {
	tmpl := `<!DOCTYPE html>
<html>
<head>
    <title>Distributed KV - {{.NodeID}}</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
        }
        .card {
            background: white;
            border-radius: 20px;
            padding: 30px;
            margin: 20px 0;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
        }
        h1 { 
            color: white;
            font-size: 2.5em;
            margin-bottom: 10px;
            text-align: center;
        }
        .subtitle {
            color: rgba(255,255,255,0.9);
            text-align: center;
            margin-bottom: 30px;
            font-size: 1.2em;
        }
        .status-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin: 20px 0;
        }
        .status-item {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }
        .status-label {
            color: #6c757d;
            font-size: 0.9em;
            margin-bottom: 8px;
        }
        .status-value {
            font-size: 2em;
            font-weight: bold;
            color: #212529;
        }
        .badge {
            display: inline-block;
            padding: 8px 20px;
            border-radius: 20px;
            font-weight: bold;
            font-size: 1.1em;
        }
        .badge.leader {
            background: #28a745;
            color: white;
        }
        .badge.follower {
            background: #6c757d;
            color: white;
        }
        .badge.candidate {
            background: #ffc107;
            color: #212529;
        }
        .peers {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 15px;
        }
        .peer {
            background: #28a745;
            color: white;
            padding: 10px 20px;
            border-radius: 20px;
            font-size: 0.9em;
        }
        .peer.offline {
            background: #dc3545;
        }
        #status-indicator {
            width: 20px;
            height: 20px;
            border-radius: 50%;
            background: #28a745;
            display: inline-block;
            margin-left: 10px;
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🗄️ Distributed Key-Value Store</h1>
        <div class="subtitle">Node: <span id="nodeId">-</span> <span id="status-indicator"></span></div>
        
        <div class="card">
            <h2>Node Status</h2>
            <div class="status-grid">
                <div class="status-item">
                    <div class="status-label">State</div>
                    <span id="state" class="badge follower">-</span>
                </div>
                <div class="status-item">
                    <div class="status-label">Term</div>
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
        
        <div class="card">
            <h2>Cluster Peers</h2>
            <div id="peers" class="peers"></div>
        </div>
    </div>

    <script>
        function updateDashboard() {
            fetch('/api/status')
                .then(r => r.json())
                .then(data => {
                    document.getElementById('nodeId').textContent = data.NodeID;
                    
                    const stateBadge = document.getElementById('state');
                    stateBadge.textContent = data.State.toUpperCase();
                    stateBadge.className = 'badge ' + data.State.toLowerCase();
                    
                    document.getElementById('term').textContent = data.CurrentTerm;
                    document.getElementById('peerCount').textContent = 
                        data.AlivePeers.length + '/' + data.TotalPeers;
                    document.getElementById('uptime').textContent = data.Uptime;
                    
                    const peersDiv = document.getElementById('peers');
                    peersDiv.innerHTML = data.AlivePeers.map(p => 
                        '<span class="peer">' + p + ' ✓</span>'
                    ).join('');
                });
        }
        
        setInterval(updateDashboard, 1000);
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
