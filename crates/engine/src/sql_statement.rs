use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::query_engine::EngineError;

/// A parsed and validated SQL statement guaranteed to be read-only.
///
/// Can only be constructed via [`SqlStatement::try_new`], which parses the SQL
/// and rejects anything other than SELECT or EXPLAIN. This ensures that
/// unvalidated SQL cannot reach the query engine.
///
/// ```
/// use olap_engine::sql_statement::SqlStatement;
///
/// // SELECT is allowed
/// let query = SqlStatement::try_new("SELECT 1".into()).unwrap();
/// assert_eq!(query.sql(), "SELECT 1");
///
/// // INSERT is rejected
/// let err = SqlStatement::try_new("INSERT INTO t VALUES (1)".into());
/// assert!(err.is_err());
/// ```
#[derive(Debug)]
pub struct SqlStatement {
    sql: String,
}

impl SqlStatement {
    /// Parse and validate a SQL string. Returns `Err` if:
    /// - the SQL is syntactically invalid
    /// - the input contains more than one statement
    /// - the statement is not SELECT or EXPLAIN
    pub fn try_new(sql: String) -> Result<Self, EngineError> {
        let statements = Parser::parse_sql(&GenericDialect {}, &sql)
            .map_err(|e| EngineError::InvalidSql(e.to_string()))?;

        if statements.len() != 1 {
            return Err(EngineError::InvalidSql(format!(
                "expected exactly one statement, got {}",
                statements.len()
            )));
        }

        match &statements[0] {
            Statement::Query(_) => {}
            Statement::Explain { .. } => {}
            other => {
                return Err(EngineError::InvalidSql(format!(
                    "only SELECT and EXPLAIN are allowed, got: {}",
                    statement_kind(other)
                )));
            }
        }

        Ok(Self { sql })
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }
}

fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Insert { .. } => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable { .. } => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        Statement::AlterTable { .. } => "ALTER TABLE",
        _ => "unsupported statement",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_simple_select() {
        let stmt = SqlStatement::try_new("SELECT 1".into()).unwrap();
        assert_eq!(stmt.sql(), "SELECT 1");
    }

    #[test]
    fn accepts_select_with_where_and_group_by() {
        let sql = "SELECT country, AVG(temp) FROM cities WHERE temp > 10 GROUP BY country";
        SqlStatement::try_new(sql.into()).unwrap();
    }

    #[test]
    fn accepts_explain() {
        SqlStatement::try_new("EXPLAIN SELECT 1".into()).unwrap();
    }

    #[test]
    fn rejects_insert() {
        let err = SqlStatement::try_new("INSERT INTO t VALUES (1)".into()).unwrap_err();
        assert!(err.to_string().contains("INSERT"));
    }

    #[test]
    fn rejects_drop() {
        let err = SqlStatement::try_new("DROP TABLE users".into()).unwrap_err();
        assert!(err.to_string().contains("DROP"));
    }

    #[test]
    fn rejects_multiple_statements() {
        let err = SqlStatement::try_new("SELECT 1; SELECT 2".into()).unwrap_err();
        assert!(err.to_string().contains("exactly one statement"));
    }

    #[test]
    fn rejects_invalid_syntax() {
        let err = SqlStatement::try_new("SELEKT garbage".into()).unwrap_err();
        assert!(err.to_string().contains("invalid SQL"));
    }

    #[test]
    fn rejects_empty_input() {
        let err = SqlStatement::try_new("".into()).unwrap_err();
        assert!(err.to_string().contains("expected exactly one statement"));
    }
}
