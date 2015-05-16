package org.embulk.output.jdbc.setter;

import java.io.IOException;
import java.sql.SQLException;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.BatchInsert;

public class NStringColumnSetter
        extends ColumnSetter
{
    private final TimestampFormatter timestampFormatter;

    public NStringColumnSetter(BatchInsert batch, JdbcColumn column,
            TimestampFormatter timestampFormatter)
    {
        super(batch, column);
        this.timestampFormatter = timestampFormatter;
    }

    @Override
    public void booleanValue(boolean v) throws IOException, SQLException
    {
        batch.setNString(Boolean.toString(v));
    }

    @Override
    public void longValue(long v) throws IOException, SQLException
    {
        batch.setNString(Long.toString(v));
    }

    @Override
    public void doubleValue(double v) throws IOException, SQLException
    {
        batch.setNString(Double.toString(v));
    }

    @Override
    public void stringValue(String v) throws IOException, SQLException
    {
        batch.setNString(v);
    }

    @Override
    public void timestampValue(Timestamp v) throws IOException, SQLException
    {
        batch.setNString(timestampFormatter.format(v));
    }
}