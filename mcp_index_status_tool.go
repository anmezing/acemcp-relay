package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

const codebaseIndexStatusToolName = "codebase_index_status"

func validateCodebaseIndexStatusArgs(args map[string]interface{}) error {
	for key, value := range args {
		switch key {
		case "root_id":
			if value != nil {
				rootID, ok := value.(string)
				if !ok || strings.TrimSpace(rootID) == "" {
					return fmt.Errorf("root_id must be a non-empty string when provided")
				}
			}
		default:
			return fmt.Errorf("unknown argument %q for %s", key, codebaseIndexStatusToolName)
		}
	}
	return nil
}

// codebaseIndexStatusToolDefinition is a Relay-owned tool. It deliberately does
// not come from the upstream LCE tools/list response: the Relay can answer the
// status from its tenant index_jobs table even while an index is still building
// or has failed before a published root exists.
func codebaseIndexStatusToolDefinition() (json.RawMessage, error) {
	definition := map[string]interface{}{
		"name":        codebaseIndexStatusToolName,
		"description": "Report the current indexing state and progress for the authenticated tenant's project roots. Use this when a codebase tool is not ready or when the user asks whether indexing is complete. The response includes each root's state, phase, processed file counts, percentage, and failure reason when available.",
		"inputSchema": map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"root_id": map[string]interface{}{
					"type":        "string",
					"description": "Optional indexed root ID. Omit it to report all roots visible to the authenticated tenant.",
				},
			},
			"additionalProperties": false,
		},
	}
	encoded, err := json.Marshal(definition)
	return json.RawMessage(encoded), err
}

func appendCodebaseIndexStatusTool(raw json.RawMessage) (json.RawMessage, error) {
	var tools []json.RawMessage
	if err := json.Unmarshal(raw, &tools); err != nil {
		return nil, fmt.Errorf("parse filtered MCP tools: %w", err)
	}
	for _, tool := range tools {
		var metadata struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(tool, &metadata); err != nil {
			return nil, fmt.Errorf("parse MCP tool metadata: %w", err)
		}
		if strings.TrimSpace(metadata.Name) == codebaseIndexStatusToolName {
			return nil, fmt.Errorf("duplicate public MCP tool %q", codebaseIndexStatusToolName)
		}
	}
	definition, err := codebaseIndexStatusToolDefinition()
	if err != nil {
		return nil, fmt.Errorf("encode %s definition: %w", codebaseIndexStatusToolName, err)
	}
	tools = append(tools, definition)
	return json.Marshal(tools)
}
