/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.Type;
import com.youdao.analysis.util.SpeedLimiter;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.max;

public class CassandraRecordCursor
        implements RecordCursor
{
    private final List<FullCassandraType> fullCassandraTypes;
    private final ResultSet rs;
    private Row currentRow;
    private long count;
    private static final Logger log = Logger.get(CassandraRecordCursor.class);
    private static SpeedLimiter speedLimiter = new SpeedLimiter(Integer.parseInt(System.getProperty("CASSANDRA_LIMIT_PER_SEC", "1000")), new SpeedLimiter.ProcessFunction()
    {
        @Override
        public void process(Object o)
        {
            //do nothing
        }
    });

    public CassandraRecordCursor(CassandraSession cassandraSession, List<FullCassandraType> fullCassandraTypes, String cql)
    {
        this.fullCassandraTypes = fullCassandraTypes;
        rs = cassandraSession.execute(cql);
        currentRow = null;
    }

    @Override
    public boolean advanceNextPosition()
    {
        // hamlet-lee: just for limit speed
        speedLimiter.process(null);

        boolean isExhausted = false;
        Throwable t = null;
        int maxTry = 10;
        int nTry;
        for (nTry = 0; nTry < maxTry; nTry++) {
            try {
                if (nTry > 0) {
                    log.info("retry " + nTry);
                }
                isExhausted = rs.isExhausted();
                break;
            }
            catch (Exception e) {
                log.error("error, sleep 3 sec for retry", e);
                try {
                    Thread.sleep(3000L);
                }
                catch (InterruptedException e1) {

                }
                t = e;
            }
        }
        if (nTry == maxTry) {
            throw new RuntimeException(t);
        }
        if (!isExhausted) {
            currentRow = rs.one();
            count++;
            return true;
        }
        return false;
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean getBoolean(int i)
    {
        return currentRow.getBool(i);
    }

    @Override
    public long getCompletedBytes()
    {
        return count;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public double getDouble(int i)
    {
        switch (getCassandraType(i)) {
            case DOUBLE:
                return currentRow.getDouble(i);
            case FLOAT:
                return currentRow.getFloat(i);
            case DECIMAL:
                return currentRow.getDecimal(i).doubleValue();
            default:
                throw new IllegalStateException("Cannot retrieve double for " + getCassandraType(i));
        }
    }

    @Override
    public long getLong(int i)
    {
        switch (getCassandraType(i)) {
            case INT:
                return currentRow.getInt(i);
            case BIGINT:
            case COUNTER:
                return currentRow.getLong(i);
            case TIMESTAMP:
                return currentRow.getTimestamp(i).getTime();
            case FLOAT:
                return floatToRawIntBits(currentRow.getFloat(i));
            default:
                throw new IllegalStateException("Cannot retrieve long for " + getCassandraType(i));
        }
    }

    private CassandraType getCassandraType(int i)
    {
        return fullCassandraTypes.get(i).getCassandraType();
    }

    @Override
    public Slice getSlice(int i)
    {
        NullableValue value = CassandraType.getColumnValue(currentRow, i, fullCassandraTypes.get(i));
        if (value.getValue() instanceof Slice) {
            return (Slice) value.getValue();
        }
        return utf8Slice(value.getValue().toString());
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getType(int i)
    {
        return getCassandraType(i).getNativeType();
    }

    @Override
    public boolean isNull(int i)
    {
        return currentRow.isNull(i);
    }
}
