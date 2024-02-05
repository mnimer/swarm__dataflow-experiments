package com.mikenimer.swarm.bqToJdbc.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.mikenimer.swarm.bqToJdbc.JobOptions;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;


/**
 * wrap the JdbcIO call into a class to help organize helper methods
 */
public class JdbcWriter extends PTransform<PCollection<TableRow>, PDone> {

    String table;
    String database;
    List<String> columns;


    public JdbcWriter(String table, String destDatabase, List<String> columns) {
        this.table = table;
        this.database = destDatabase;
        this.columns = columns;
    }

    @Override
    public PDone expand(PCollection<TableRow> input) {

        JobOptions options = input.getPipeline().getOptions().as(JobOptions.class);

        String jdbcTable = String.format("%s.%s", database, table.split("\\.")[2]);
        String insertSql = createInsertSql(jdbcTable, columns);

        // TODO: Try .writeVoid and .writeWithResults
        return input.apply("Write to db", JdbcIO.<TableRow>write().withTable(jdbcTable)
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(options.getDriver(), options.getUrl())
                        .withUsername(options.getUsername())
                        .withPassword(options.getPassword()))
                .withStatement(insertSql)
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {
                    @Override
                    public void setParameters(TableRow element, @UnknownKeyFor @NonNull @Initialized PreparedStatement preparedStatement) throws @UnknownKeyFor @NonNull @Initialized Exception {
                        setValuesByType(element, preparedStatement, columns);
                    }
                }));

    }


    /**
     * Create sql insert string, with ? char for every column
     * @param table
     * @param columns
     * @return
     */
    private String createInsertSql(String table, List<String> columns) {
        String[] param = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            param[i] = "?";
        }
        String insertSql = String.format("insert into %s values(%s)", table, String.join(",", Arrays.asList(param)));
        return insertSql;
    }


    /**
     * Real simple casted, grab named column out of TableRow using column list, check java type, set in PreparedStatement with type. 
     * 
     * TODO: make this more durable, add error handling, maybe go to SQL to get JDBC type to make sure cast is correct.
     * TODO: Map the rest of the values. You might want to pass in SQL DB schema to do any type conversions in line here.
     * 
     * @param element
     * @param preparedStatement
     * @param columns
     * @throws SQLException
     */
    private void setValuesByType(TableRow element, PreparedStatement preparedStatement, List<String> columns) throws SQLException {
        
        for (int i = 0; i <= columns.size(); i++) {
            String name = columns.get(i);
            if (element.get(name) instanceof Integer) {
                preparedStatement.setInt(i+1, (Integer) element.get(name));
            } else if (element.get(name) instanceof Float) {
                preparedStatement.setFloat(i+1, (Float) element.get(name));
            } else if (element.get(name) instanceof Double) {
                preparedStatement.setDouble(i+1, (Double) element.get(name));
            } else if (element.get(name) instanceof Boolean) {
                preparedStatement.setBoolean(i+1, (Boolean) element.get(name));
            } else
                //set default as string
                preparedStatement.setString(i+1, element.get(name).toString());
        }
    }
}
