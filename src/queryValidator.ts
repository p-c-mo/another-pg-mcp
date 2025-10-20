import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';

export interface ValidationResult {
  isValid: boolean;
  error?: string;
}

export class QueryValidator {
  private readOnlyMode: boolean;
  
  // Patterns for dangerous commands that modify system settings
  private static readonly SYSTEM_COMMANDS = [
    /^\s*SET\s+/i,
    /;\s*SET\s+/i,
    /\n\s*SET\s+/i,
    /^\s*RESET\s+/i,
    /;\s*RESET\s+/i,
    /\n\s*RESET\s+/i,
  ];
  
  // Patterns for DML commands that modify data
  private static readonly DML_COMMANDS = [
    /^\s*INSERT\s+/i,
    /;\s*INSERT\s+/i,
    /\n\s*INSERT\s+/i,
    /^\s*UPDATE\s+/i,
    /;\s*UPDATE\s+/i,
    /\n\s*UPDATE\s+/i,
    /^\s*DELETE\s+/i,
    /;\s*DELETE\s+/i,
    /\n\s*DELETE\s+/i,
    /^\s*MERGE\s+/i,
    /;\s*MERGE\s+/i,
    /\n\s*MERGE\s+/i,
  ];
  
  // Patterns for DDL commands that modify schema
  private static readonly DDL_COMMANDS = [
    /^\s*CREATE\s+/i,
    /;\s*CREATE\s+/i,
    /\n\s*CREATE\s+/i,
    /^\s*ALTER\s+/i,
    /;\s*ALTER\s+/i,
    /\n\s*ALTER\s+/i,
    /^\s*DROP\s+/i,
    /;\s*DROP\s+/i,
    /\n\s*DROP\s+/i,
    /^\s*TRUNCATE\s+/i,
    /;\s*TRUNCATE\s+/i,
    /\n\s*TRUNCATE\s+/i,
  ];
  
  // Patterns for other potentially dangerous commands
  private static readonly ADMIN_COMMANDS = [
    /^\s*GRANT\s+/i,
    /;\s*GRANT\s+/i,
    /\n\s*GRANT\s+/i,
    /^\s*REVOKE\s+/i,
    /;\s*REVOKE\s+/i,
    /\n\s*REVOKE\s+/i,
    /^\s*VACUUM\s+/i,
    /;\s*VACUUM\s+/i,
    /\n\s*VACUUM\s+/i,
    /^\s*ANALYZE\s+/i,
    /;\s*ANALYZE\s+/i,
    /\n\s*ANALYZE\s+/i,
    /^\s*CLUSTER\s+/i,
    /;\s*CLUSTER\s+/i,
    /\n\s*CLUSTER\s+/i,
    /^\s*REINDEX\s+/i,
    /;\s*REINDEX\s+/i,
    /\n\s*REINDEX\s+/i,
    /^\s*REFRESH\s+MATERIALIZED\s+VIEW\s+/i,
    /;\s*REFRESH\s+MATERIALIZED\s+VIEW\s+/i,
    /\n\s*REFRESH\s+MATERIALIZED\s+VIEW\s+/i,
  ];

  constructor(readOnlyMode: boolean = true) {
    this.readOnlyMode = readOnlyMode;
  }

  validate(query: string): ValidationResult {
    // Always block system commands (SET, RESET)
    for (const pattern of QueryValidator.SYSTEM_COMMANDS) {
      if (pattern.test(query)) {
        return {
          isValid: false,
          error: 'System configuration commands (SET, RESET) are not allowed'
        };
      }
    }
    
    // In read-only mode, block all data modification commands
    if (this.readOnlyMode) {
      // Check DML commands
      for (const pattern of QueryValidator.DML_COMMANDS) {
        if (pattern.test(query)) {
          return {
            isValid: false,
            error: 'Data modification commands (INSERT, UPDATE, DELETE, MERGE) are not allowed in read-only mode'
        };
        }
      }
      
      // Check DDL commands
      for (const pattern of QueryValidator.DDL_COMMANDS) {
        if (pattern.test(query)) {
          return {
            isValid: false,
            error: 'Schema modification commands (CREATE, ALTER, DROP, TRUNCATE) are not allowed in read-only mode'
          };
        }
      }
      
      // Check admin commands
      for (const pattern of QueryValidator.ADMIN_COMMANDS) {
        if (pattern.test(query)) {
          return {
            isValid: false,
            error: 'Administrative commands (GRANT, REVOKE, VACUUM, ANALYZE, etc.) are not allowed in read-only mode'
          };
        }
      }
    }
    
    return { isValid: true };
  }
  
  validateOrThrow(query: string): void {
    const result = this.validate(query);
    if (!result.isValid) {
      throw new McpError(
        ErrorCode.InvalidParams,
        result.error || 'Query validation failed'
      );
    }
  }
}