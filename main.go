package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/ssh"
)

type SchemaPermissions map[string]bool

type Config struct {
	MySQL MySQLConfig
	SSH   SSHConfig
}

type MySQLConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

type SSHConfig struct {
	Enabled    bool
	Host       string
	Port       string
	User       string
	Password   string
	PrivateKey []byte
}

type Server struct {
	cfg Config

	allowInsert bool
	allowUpdate bool
	allowDelete bool
	allowDDL    bool

	schemaInsertPermissions SchemaPermissions
	schemaUpdatePermissions SchemaPermissions
	schemaDeletePermissions SchemaPermissions
	schemaDDLPermissions    SchemaPermissions

	db             *sql.DB
	tunnelListener net.Listener
	mu             sync.Mutex
}

type ToolResult struct {
	Content []ToolContent `json:"content"`
	IsError bool          `json:"isError"`
}

type ToolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      any           `json:"id,omitempty"`
	Result  any           `json:"result,omitempty"`
	Error   *jsonRPCError `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpToolCallParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

var (
	useRegexp     = regexp.MustCompile(`(?i)\buse\s+` + "`?" + `([a-zA-Z0-9_]+)` + "`?")
	dbTableRegexp = regexp.MustCompile("`?([a-zA-Z0-9_]+)`?\\.`?[a-zA-Z0-9_]+`?")
)

func main() {
	srv, err := NewServerFromEnv()
	if err != nil {
		logErr("failed to create server:", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		logErr("failed to start server:", err)
		os.Exit(1)
	}
	defer srv.Close()

	httpAddr := ":" + defaultString("HTTP_PORT", "8080")
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/mcp", srv.handleMCP)

	httpServer := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	go handleSignals(cancel, srv, httpServer)

	logInfo("server ready: streamable HTTP endpoint on", httpAddr, "(POST /mcp)")
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logErr("http server error:", err)
		os.Exit(1)
	}
}

func NewServerFromEnv() (*Server, error) {
	cfg, err := loadConfigFromEnv()
	if err != nil {
		return nil, err
	}

	return &Server{
		cfg:                     cfg,
		allowInsert:             envBool("ALLOW_INSERT_OPERATION", false),
		allowUpdate:             envBool("ALLOW_UPDATE_OPERATION", false),
		allowDelete:             envBool("ALLOW_DELETE_OPERATION", false),
		allowDDL:                envBool("ALLOW_DDL_OPERATION", false),
		schemaInsertPermissions: parseSchemaPermissions(os.Getenv("SCHEMA_INSERT_PERMISSIONS")),
		schemaUpdatePermissions: parseSchemaPermissions(os.Getenv("SCHEMA_UPDATE_PERMISSIONS")),
		schemaDeletePermissions: parseSchemaPermissions(os.Getenv("SCHEMA_DELETE_PERMISSIONS")),
		schemaDDLPermissions:    parseSchemaPermissions(os.Getenv("SCHEMA_DDL_PERMISSIONS")),
	}, nil
}

func loadConfigFromEnv() (Config, error) {
	sshKeyPath := strings.TrimSpace(os.Getenv("SSH_KEY"))
	var key []byte
	if sshKeyPath != "" {
		b, err := os.ReadFile(sshKeyPath)
		if err != nil {
			return Config{}, fmt.Errorf("read SSH_KEY failed: %w", err)
		}
		key = b
	}

	cfg := Config{
		MySQL: MySQLConfig{
			Host:     defaultString("MYSQL_HOST", "127.0.0.1"),
			Port:     defaultString("MYSQL_PORT", "3306"),
			User:     defaultString("MYSQL_USER", "root"),
			Password: defaultString("MYSQL_PASS", "root"),
			Database: strings.TrimSpace(os.Getenv("MYSQL_DB")),
		},
		SSH: SSHConfig{
			Enabled:    strings.TrimSpace(os.Getenv("SSH_HOST")) != "" && strings.TrimSpace(os.Getenv("SSH_USER")) != "",
			Host:       strings.TrimSpace(os.Getenv("SSH_HOST")),
			Port:       defaultString("SSH_PORT", "22"),
			User:       strings.TrimSpace(os.Getenv("SSH_USER")),
			Password:   strings.TrimSpace(os.Getenv("SSH_PASS")),
			PrivateKey: key,
		},
	}

	return cfg, nil
}

func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		return nil
	}

	host := s.cfg.MySQL.Host
	port := s.cfg.MySQL.Port

	if s.cfg.SSH.Enabled {
		localPort, ln, err := createSSHTunnel(ctx, s.cfg.SSH, host, port)
		if err != nil {
			return fmt.Errorf("create SSH tunnel failed: %w", err)
		}
		s.tunnelListener = ln
		host = "127.0.0.1"
		port = localPort
		logInfo("SSH tunnel established on", host+":"+port)
	}

	dsn := buildDSN(s.cfg.MySQL.User, s.cfg.MySQL.Password, host, port, s.cfg.MySQL.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open mysql failed: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return fmt.Errorf("mysql ping failed: %w", err)
	}

	s.db = db
	logInfo("MySQL connection is ready")
	return nil
}

func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		_ = s.db.Close()
		s.db = nil
	}
	if s.tunnelListener != nil {
		_ = s.tunnelListener.Close()
		s.tunnelListener = nil
	}
}

func (s *Server) ExecuteReadOnlyQuery(ctx context.Context, sqlText string) (ToolResult, error) {
	queryTypes := getQueryTypes(sqlText)
	schema := extractSchemaFromQuery(sqlText, s.cfg.MySQL.Database)

	isInsert := contains(queryTypes, "insert")
	isUpdate := contains(queryTypes, "update")
	isDelete := contains(queryTypes, "delete")
	isDDL := hasAny(queryTypes, []string{"create", "alter", "drop", "truncate"})

	if isInsert && !s.isInsertAllowedForSchema(schema) {
		return denyResult("INSERT", schema, "SCHEMA_INSERT_PERMISSIONS"), nil
	}
	if isUpdate && !s.isUpdateAllowedForSchema(schema) {
		return denyResult("UPDATE", schema, "SCHEMA_UPDATE_PERMISSIONS"), nil
	}
	if isDelete && !s.isDeleteAllowedForSchema(schema) {
		return denyResult("DELETE", schema, "SCHEMA_DELETE_PERMISSIONS"), nil
	}
	if isDDL && !s.isDDLAllowedForSchema(schema) {
		return denyResult("DDL", schema, "SCHEMA_DDL_PERMISSIONS"), nil
	}

	if isInsert || isUpdate || isDelete || isDDL {
		return s.executeWriteQuery(ctx, sqlText, schema, queryTypes)
	}

	rows, err := s.executeRowsQuery(ctx, sqlText)
	if err != nil {
		return ToolResult{}, err
	}
	jsonBytes, _ := json.MarshalIndent(rows, "", "  ")
	return ToolResult{
		Content: []ToolContent{{Type: "text", Text: string(jsonBytes)}},
		IsError: false,
	}, nil
}

func (s *Server) executeWriteQuery(ctx context.Context, sqlText, schema string, queryTypes []string) (ToolResult, error) {
	res, err := s.db.ExecContext(ctx, strings.ToLower(sqlText))
	if err != nil {
		return ToolResult{
			Content: []ToolContent{{Type: "text", Text: "Error executing write operation: " + err.Error()}},
			IsError: true,
		}, nil
	}

	affected, _ := res.RowsAffected()
	lastID, _ := res.LastInsertId()
	schemaName := defaultSchemaLabel(schema)

	var msg string
	switch {
	case contains(queryTypes, "insert"):
		msg = fmt.Sprintf("Insert successful on schema '%s'. Affected rows: %d, Last insert ID: %d", schemaName, affected, lastID)
	case contains(queryTypes, "update"):
		msg = fmt.Sprintf("Update successful on schema '%s'. Affected rows: %d", schemaName, affected)
	case contains(queryTypes, "delete"):
		msg = fmt.Sprintf("Delete successful on schema '%s'. Affected rows: %d", schemaName, affected)
	case hasAny(queryTypes, []string{"create", "alter", "drop", "truncate"}):
		msg = fmt.Sprintf("DDL operation successful on schema '%s'.", schemaName)
	default:
		msg = "Write operation successful"
	}

	return ToolResult{Content: []ToolContent{{Type: "text", Text: msg}}, IsError: false}, nil
}

func (s *Server) executeRowsQuery(ctx context.Context, sqlText string) ([]map[string]any, error) {
	rows, err := s.db.QueryContext(ctx, strings.ToLower(sqlText))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := make([]map[string]any, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]any, len(columns))
		for i, c := range columns {
			switch v := values[i].(type) {
			case []byte:
				rowMap[c] = string(v)
			default:
				rowMap[c] = v
			}
		}
		result = append(result, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func handleSignals(cancel context.CancelFunc, srv *Server, httpServer *http.Server) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	srv.Close()
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req jsonRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, jsonRPCResponse{
			JSONRPC: "2.0",
			Error:   &jsonRPCError{Code: -32700, Message: "parse error"},
		})
		return
	}

	res, hasResponse := s.dispatchMCPRequest(r.Context(), req)
	if !hasResponse {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) dispatchMCPRequest(ctx context.Context, req jsonRPCRequest) (jsonRPCResponse, bool) {
	resp := jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
	}

	if req.Method == "notifications/initialized" && req.ID == nil {
		return jsonRPCResponse{}, false
	}

	switch req.Method {
	case "initialize":
		resp.Result = map[string]any{
			"protocolVersion": "2025-03-26",
			"capabilities": map[string]any{
				"tools": map[string]any{},
			},
			"serverInfo": map[string]any{
				"name":    "mcp-mysql",
				"version": "0.1.0",
			},
		}
	case "ping":
		resp.Result = map[string]any{}
	case "tools/list":
		resp.Result = map[string]any{
			"tools": []map[string]any{
				{
					"name":        "query",
					"description": "Execute SQL query against MySQL with schema-based write permissions.",
					"inputSchema": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"sql": map[string]any{
								"type":        "string",
								"description": "SQL statement to execute.",
							},
						},
						"required": []string{"sql"},
					},
				},
			},
		}
	case "tools/call":
		var params mcpToolCallParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			resp.Error = &jsonRPCError{Code: -32602, Message: "invalid params"}
			return resp, true
		}
		if params.Name != "query" {
			resp.Error = &jsonRPCError{Code: -32602, Message: "unsupported tool name"}
			return resp, true
		}

		sqlText, ok := readSQLArgument(params.Arguments)
		if !ok {
			resp.Error = &jsonRPCError{Code: -32602, Message: "sql argument is required"}
			return resp, true
		}

		toolRes, err := s.ExecuteReadOnlyQuery(ctx, sqlText)
		if err != nil {
			toolRes = ToolResult{
				Content: []ToolContent{{Type: "text", Text: err.Error()}},
				IsError: true,
			}
		}
		resp.Result = toolRes
	default:
		resp.Error = &jsonRPCError{Code: -32601, Message: "method not found"}
	}

	return resp, true
}

func readSQLArgument(args map[string]any) (string, bool) {
	if len(args) == 0 {
		return "", false
	}
	if v, ok := args["sql"].(string); ok && strings.TrimSpace(v) != "" {
		return v, true
	}
	if v, ok := args["query"].(string); ok && strings.TrimSpace(v) != "" {
		return v, true
	}
	return "", false
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func parseSchemaPermissions(s string) SchemaPermissions {
	permissions := make(SchemaPermissions)
	if strings.TrimSpace(s) == "" {
		return permissions
	}

	pairs := strings.Split(s, ",")
	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) != 2 {
			continue
		}
		schema := strings.TrimSpace(parts[0])
		value := strings.EqualFold(strings.TrimSpace(parts[1]), "true")
		if schema != "" {
			permissions[schema] = value
		}
	}
	return permissions
}

func (s *Server) isInsertAllowedForSchema(schema string) bool {
	if schema == "" {
		return s.allowInsert
	}
	v, ok := s.schemaInsertPermissions[schema]
	if ok {
		return v
	}
	return s.allowInsert
}

func (s *Server) isUpdateAllowedForSchema(schema string) bool {
	if schema == "" {
		return s.allowUpdate
	}
	v, ok := s.schemaUpdatePermissions[schema]
	if ok {
		return v
	}
	return s.allowUpdate
}

func (s *Server) isDeleteAllowedForSchema(schema string) bool {
	if schema == "" {
		return s.allowDelete
	}
	v, ok := s.schemaDeletePermissions[schema]
	if ok {
		return v
	}
	return s.allowDelete
}

func (s *Server) isDDLAllowedForSchema(schema string) bool {
	if schema == "" {
		return s.allowDDL
	}
	v, ok := s.schemaDDLPermissions[schema]
	if ok {
		return v
	}
	return s.allowDDL
}

func extractSchemaFromQuery(sqlText, defaultSchema string) string {
	if m := useRegexp.FindStringSubmatch(sqlText); len(m) > 1 {
		return m[1]
	}
	if m := dbTableRegexp.FindStringSubmatch(sqlText); len(m) > 1 {
		return m[1]
	}
	return defaultSchema
}

func getQueryTypes(sqlText string) []string {
	parts := strings.Split(sqlText, ";")
	types := make([]string, 0, len(parts))

	for _, p := range parts {
		q := strings.TrimSpace(strings.ToLower(p))
		if q == "" {
			continue
		}
		fields := strings.Fields(q)
		if len(fields) == 0 {
			continue
		}
		types = append(types, fields[0])
	}

	if len(types) == 0 {
		return []string{"unknown"}
	}
	return types
}

func createSSHTunnel(ctx context.Context, cfg SSHConfig, targetHost, targetPort string) (string, net.Listener, error) {
	auth := make([]ssh.AuthMethod, 0, 2)
	if cfg.Password != "" {
		auth = append(auth, ssh.Password(cfg.Password))
	}
	if len(cfg.PrivateKey) > 0 {
		signer, err := ssh.ParsePrivateKey(cfg.PrivateKey)
		if err != nil {
			return "", nil, fmt.Errorf("parse private key failed: %w", err)
		}
		auth = append(auth, ssh.PublicKeys(signer))
	}
	if len(auth) == 0 {
		return "", nil, errors.New("ssh auth is required: SSH_PASS or SSH_KEY")
	}

	sshCfg := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            auth,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	client, err := ssh.Dial("tcp", net.JoinHostPort(cfg.Host, cfg.Port), sshCfg)
	if err != nil {
		return "", nil, fmt.Errorf("ssh dial failed: %w", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = client.Close()
		return "", nil, fmt.Errorf("local listen failed: %w", err)
	}

	go func() {
		<-ctx.Done()
		_ = ln.Close()
		_ = client.Close()
	}()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(local net.Conn) {
				defer local.Close()

				remote, err := client.Dial("tcp", net.JoinHostPort(targetHost, targetPort))
				if err != nil {
					return
				}
				defer remote.Close()

				go io.Copy(remote, local)
				_, _ = io.Copy(local, remote)
			}(conn)
		}
	}()

	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		_ = ln.Close()
		_ = client.Close()
		return "", nil, err
	}
	return port, ln, nil
}

func buildDSN(user, pass, host, port, db string) string {
	dbPart := db
	if dbPart == "" {
		dbPart = ""
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4", user, pass, host, port, dbPart)
}

func denyResult(op, schema, permissionKey string) ToolResult {
	schemaLabel := defaultSchemaLabel(schema)
	return ToolResult{
		Content: []ToolContent{{
			Type: "text",
			Text: fmt.Sprintf("Error: %s operations are not allowed for schema '%s'. Ask the administrator to update %s.", op, schemaLabel, permissionKey),
		}},
		IsError: true,
	}
}

func defaultSchemaLabel(schema string) string {
	if strings.TrimSpace(schema) == "" {
		return "default"
	}
	return schema
}

func contains(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}

func hasAny(values []string, targets []string) bool {
	for _, t := range targets {
		if contains(values, t) {
			return true
		}
	}
	return false
}

func envBool(key string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch v {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func defaultString(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func logInfo(args ...any) {
	if envBool("ENABLE_LOGGING", false) {
		fmt.Fprintln(os.Stderr, args...)
	}
}

func logErr(args ...any) {
	if envBool("ENABLE_LOGGING", false) {
		fmt.Fprintln(os.Stderr, args...)
	}
}
