package org.apache.ignite.internal.processors.query.h2.sql;

public class GridSqlDropSchema extends GridSqlStatement {
    /** Schema name. */
    private String schemaName;

    /** Quietly ignore this command if schema doest exists. */
    private boolean ifExists;

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     */
    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Quietly ignore this command if schema doesn't exists.
     */
    public boolean ifExists() {
        return ifExists;
    }

    /**
     * @param ifExists Quietly ignore this command if schema doesn't exists.
     */
    public void ifExists(boolean ifExists) {
        this.ifExists = ifExists;
    }


    /** {@inheritDoc} */
    @Override public String getSQL() {
        return null;
    }
}
