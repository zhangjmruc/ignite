package org.apache.ignite.internal.processors.query.h2.sql;

public class GridSqlCreateSchema extends GridSqlStatement {
    /** Schema name. */
    private String schemaName;

    /** Quietly ignore this command if schema already exists. */
    private boolean ifNotExists;

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
     * @return Quietly ignore this command if schema already exists.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * @param ifNotExists Quietly ignore this command if schema already exists.
     */
    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }


    /** {@inheritDoc} */
    @Override public String getSQL() {
        return null;
    }
}
