package com.mikenimer.swarm.windows.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.mikenimer.swarm.windows.JobOptions;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcWriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;


/**
 * wrap the JdbcIO call into a class to help organize helper methods
 */
public class JdbcWriter extends PTransform<PCollection<TableRow>, PCollection<JdbcWriteResult>> {

    JobOptions options;

    public JdbcWriter(JobOptions options) {
        this.options = options;
    }

    @Override
    public PCollection<JdbcWriteResult> expand(PCollection<TableRow> input) {

        String insertSql = createInsertSql();


        return input.apply("Write to db", JdbcIO.<TableRow>write().withTable(options.getDestTable())
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(options.getDriver(), options.getUrl())
                        .withUsername(options.getUsername())
                        .withPassword(options.getPassword()))
                .withStatement(insertSql).withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {
                    @Override
                    public void setParameters(TableRow element, @UnknownKeyFor @NonNull @Initialized PreparedStatement preparedStatement) throws @UnknownKeyFor @NonNull @Initialized Exception {
                        setValuesByType(element, preparedStatement);
                    }

                }).withWriteResults(new JdbcIO.RowMapper<JdbcWriteResult>() {
                    @Override
                    public JdbcWriteResult mapRow(@UnknownKeyFor @NonNull @Initialized ResultSet resultSet) throws @UnknownKeyFor @NonNull @Initialized Exception {
//                             JdbcWriteResult.create()
//                                     .withUpdateCount(resultSet.getUpdateCount())
//                                     .withGeneratedKeys(resultSet.getGeneratedKeys());
                        return null;
                    }
                }));

    }

    private String createInsertSql() {
        String[] param = new String[options.getDestColumnList().length];
        for (int i = 0; i < options.getDestColumnList().length; i++) {
            param[i] = "?";
        }
        String insertSql = String.format("insert into %s values(%s)", options.getDestTable(), String.join(",", Arrays.asList(param)));
        return insertSql;
    }

    private void setValuesByType(TableRow element, PreparedStatement preparedStatement) throws SQLException {
        // TODO: Map the rest of the values. You might want to pass in SQL DB schema to do any type conversions in line here.
        for (int i = 1; i <= options.getDestColumnList().length; i++) {
            String name = options.getDestColumnList()[i];
            if (element.get(name) instanceof Integer) {
                preparedStatement.setInt(i, (Integer) element.get(name));
            } else if (element.get(name) instanceof Float) {
                preparedStatement.setFloat(i, (Float) element.get(name));
            } else if (element.get(name) instanceof Double) {
                preparedStatement.setDouble(i, (Double) element.get(name));
            } else if (element.get(name) instanceof Boolean) {
                preparedStatement.setBoolean(i, (Boolean) element.get(name));
            } else
                //set default as string
                preparedStatement.setString(i, element.get(name).toString());
        }
    }
}
