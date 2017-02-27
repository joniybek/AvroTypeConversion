package me.joniybek.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;

public class AvroTypeConversionTest {

    static GenericData.Record prepareRecord( Object record, Schema schema ) {
        try {
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            final Encoder encoder = EncoderFactory.get().binaryEncoder( os, null );
            final DatumWriter<Object> recordWriter =
                    ReflectData.get().createDatumWriter( AvroTypeConversionTest.writerSchema );

            recordWriter.write( record, encoder );
            encoder.flush();

            final Decoder decoder = DecoderFactory.get().binaryDecoder( os.toByteArray(), null );
            final GenericDatumReader<GenericData.Record> recordReader = new GenericDatumReader<>( schema );

            return recordReader.read( null, decoder );
        } catch( IOException e ) {
            throw new RuntimeException( "Error converting Avro record: " + e.getMessage(), e );
        }
    }

    public static class Record {
        private final String              col1;
        @Nullable
        private final Long                col2;
        private final SubRecord           col3;
        private final String[]            col4;
        private final Map<String, String> col5;
        private final EnumType            col6;
        private final byte[]              col7;

        public Record( String col1, Long col2, SubRecord col3, String[] col4, HashMap<String, String> col5,
                EnumType col6, byte[] col7 ) {
            this.col1 = col1;
            this.col2 = col2;
            this.col3 = col3;
            this.col4 = col4;
            this.col5 = col5;
            this.col6 = col6;
            this.col7 = col7;
        }
    }

    public static class SubRecord {
        private final String inCol1;

        public SubRecord( String inCol1 ) {
            this.inCol1 = inCol1;
        }
    }

    public static enum EnumType {
        A1, b2, c_3
    }

    private static Schema              readerSchema;
    private static Schema              writerSchema;
    private static GenericRecordType   recordType;
    private static List<Record>        records;
    private static List<GenericRecord> testData;

    @Before
    public void setup() {
        writerSchema = ReflectData.get().getSchema( Record.class );
        writerSchema.getFields().get( 1 ).addAlias( "alias1" );

        Schema tmp = ReflectData.get().getSchema( Record.class );
        readerSchema = Schema.createRecord(
                Arrays.asList(
                        new Schema.Field( "notCol1", tmp.getField( "col1" ).schema(), "doc", null ),
                        new Schema.Field( "notCol2", tmp.getField( "col2" ).schema(), "doc", 0L ),
                        new Schema.Field( "col3", tmp.getField( "col3" ).schema(), "doc", null )

                ) );
        readerSchema.getFields().get( 0 ).addAlias( "col1" );
        readerSchema.getFields().get( 1 ).addAlias( "alias1" );

        recordType = GenericRecordType.of( writerSchema );

        records = new ArrayList<Record>();

        LongStream.range( 1, 6 ).forEach( x -> {
            records.add( new Record( "record" + x, x,
                    new SubRecord( "innerRec" + x ),
                    new String[] { "1", "2", "1", "4" },
                    new HashMap<String, String>() {
                        {
                            put( "key", String.valueOf( x ) );
                        }
                    },
                    EnumType.c_3,
                    "test".getBytes() )

            );
        } );

        testData = records.stream().map( r -> prepareRecord( r, writerSchema ) ).collect( Collectors.toList() );

        Assert.assertNotNull( testData );

    }

    @Test
    public void getGenRecFieldValue_objectCreation() throws Exception {
        Object object = AvroTypeConversion.getGenRecFieldValue( writerSchema.getField( "col1" ), testData.get( 0 ) );
        Assert.assertNotNull( object );
        Assert.assertThat( object instanceof Optional, is( true ) );
        Assert.assertThat( ( (Optional) object ).isPresent(), is( true ) );
        Assert.assertThat( ( (Optional) object ).get() instanceof CharSequence, is( true ) );
        Assert.assertThat( ( (Utf8) ( (Optional) object ).get() ).toString(), is( "record1" ) );
    }

    @Test
    public void getGenRecFieldValue_aliasesSupport() throws Exception {
        Object object = AvroTypeConversion.getGenRecFieldValue( readerSchema.getField( "notCol1" ), testData.get( 0 ) );
        Assert.assertNotNull( object );
        Assert.assertThat( object instanceof Optional, is( true ) );
        Assert.assertThat( ( (Optional) object ).isPresent(), is( true ) );
        Assert.assertThat( ( (Optional) object ).get() instanceof CharSequence, is( true ) );
        Assert.assertThat( ( (Utf8) ( (Optional) object ).get() ).toString(), is( "record1" ) );

        Object object2 =
                AvroTypeConversion.getGenRecFieldValue( readerSchema.getField( "notCol2" ), testData.get( 0 ) );
        Assert.assertNotNull( object2 );
        Assert.assertThat( object2 instanceof Optional, is( true ) );
        Assert.assertThat( ( (Optional) object2 ).isPresent(), is( true ) );
        Assert.assertThat( ( (Optional) object2 ).get() instanceof Long, is( true ) );
        Assert.assertThat( (Long) ( (Optional) object2 ).get(), is( 1L ) );
    }

    @Test
    public void convertBool_succ() throws Exception {
        Assert.assertThat( AvroTypeConversion.convertBool( Boolean.TRUE, readerSchema ), is( true ) );
        Assert.assertThat( AvroTypeConversion.convertBool( "True", readerSchema ), is( true ) );
        Assert.assertThat( AvroTypeConversion.convertBool( "TRUE", readerSchema ), is( true ) );
        Assert.assertThat( AvroTypeConversion.convertBool( new Utf8( "true" ), readerSchema ), is( true ) );
        Assert.assertThat( AvroTypeConversion.convertBool( "true", readerSchema ), is( true ) );
        Assert.assertThat( AvroTypeConversion.convertBool( new Utf8( "true" ), readerSchema ), is( true ) );

        Assert.assertThat( AvroTypeConversion.convertBool( Boolean.FALSE, readerSchema ), is( false ) );
        Assert.assertThat( AvroTypeConversion.convertBool( "False", readerSchema ), is( false ) );
        Assert.assertThat( AvroTypeConversion.convertBool( "FALSE", readerSchema ), is( false ) );

    }

    @Test(
            expected = Exception.class )
    public void convertBool_fail() throws Exception {
        try {
            AvroTypeConversion.convertBool( "Cannot cast me", readerSchema );
        } catch( ClassCastException e ) {
            // Check exception msg
            Assert.assertThat( e.getMessage(), containsString( "to RECORD" ) );
        }
        AvroTypeConversion.convertBool( "Cannot cast me", readerSchema );
    }

    @Test
    public void convertUnion_succ() throws Exception {
        Optional<Object> object =
                AvroTypeConversion.getGenRecFieldValue( writerSchema.getField( "col2" ), testData.get( 0 ) );
        Assert.assertNotNull( AvroTypeConversion.convertUnion( object.get(), Schema.createUnion(
                Schema.create( Schema.Type.LONG ), Schema.create( Schema.Type.NULL ) ), false ) );
        Assert.assertThat( AvroTypeConversion.convertUnion( object.get(), Schema.createUnion(
                Schema.create( Schema.Type.LONG ), Schema.create( Schema.Type.NULL ) ), false ) instanceof Long,
                is( true ) );
        Assert.assertThat( AvroTypeConversion.convertUnion( object.get(), Schema.createUnion(
                Schema.create( Schema.Type.NULL ), Schema.create( Schema.Type.LONG ) ), false ) instanceof Long,
                is( true ) );
        Assert.assertThat( AvroTypeConversion.convertUnion( object.get(), Schema.createUnion(
                Schema.create( Schema.Type.NULL ), Schema.create( Schema.Type.INT ) ), false ), is( 1 ) );
        Assert.assertThat( AvroTypeConversion.convertUnion( object.get(), Schema.createUnion(
                Schema.create( Schema.Type.NULL ),
                Schema.create( Schema.Type.INT ),
                Schema.create( Schema.Type.LONG ) ), false ) instanceof Long, is( true ) );
        Assert.assertThat( AvroTypeConversion.convertUnion( object.get(), Schema.createUnion(
                Schema.create( Schema.Type.NULL ),
                Schema.create( Schema.Type.DOUBLE ),
                Schema.create( Schema.Type.FLOAT ) ), false ) instanceof Double, is( true ) );

        Assert.assertNull( AvroTypeConversion.convertUnion( null, Schema.createUnion(
                Schema.create( Schema.Type.LONG ), Schema.create( Schema.Type.NULL ) ), false ) );

    }

    @Test(
            expected = Exception.class )
    public void convertUnion_fail() throws Exception {
        AvroTypeConversion.convertUnion( null, Schema.create( Schema.Type.LONG ), false );
    }

    @Test
    public void convertArray_succ() throws Exception {
        Schema writer = Schema.createArray( Schema.create( Schema.Type.STRING ) );
        Schema reader = Schema.createArray( Schema.create( Schema.Type.LONG ) );
        GenericData.Array<Utf8> arr = new GenericData.Array<Utf8>( 10, writer );
        arr.add( new Utf8( "1" ) );
        arr.add( new Utf8( "2" ) );
        arr.add( new Utf8( "3" ) );

        Assert.assertNotNull( AvroTypeConversion.convertArray( arr, reader ) );
        Assert.assertThat( AvroTypeConversion.convertArray( arr, reader ).get( 0 ), is( 1L ) );

        Iterable it = AvroTypeConversion.convertArray(
                AvroTypeConversion.getGenRecFieldValue( writerSchema.getField( "col4" ), testData.get( 0 ) ).get(),
                Schema.createArray( Schema.create( Schema.Type.INT ) ) ); //Actual cast from string to INT
        Assert.assertTrue( StreamSupport.stream( it.spliterator(), false )
                .allMatch( x -> Arrays.asList( 1, 2, 3, 4 ).contains( x ) ) );

    }

    @Test(
            expected = Exception.class )
    public void convertArray_fail() throws Exception {
        Optional<Object> object =
                AvroTypeConversion.getGenRecFieldValue( writerSchema.getField( "col4" ), testData.get( 0 ) );
        AvroTypeConversion.convertArray( object.get(), Schema.createArray( Schema.create( Schema.Type.ENUM ) ) ); // Cannot cast to ENUM
    }

    @Test
    public void convertMap_succ() throws Exception {
        Optional<Object> object =
                AvroTypeConversion.getGenRecFieldValue( writerSchema.getField( "col5" ), testData.get( 0 ) );
        Assert.assertNotNull(
                AvroTypeConversion.convertMap( object.get(), Schema.createMap( Schema.create( Schema.Type.LONG ) ) ) );

        Map map = (Map) AvroTypeConversion.convertMap( object.get(),
                Schema.createMap( Schema.create( Schema.Type.LONG ) ) );
        Assert.assertNotNull( map );
        Assert.assertThat( map.containsKey( new Utf8( "key" ) ), is( true ) );
        Assert.assertThat( map.get( new Utf8( "key" ) ), is( 1L ) );

    }

    @Test(
            expected = Exception.class )
    public void convertMap_fail() throws Exception {
        Optional<Object> object =
                AvroTypeConversion.getGenRecFieldValue( writerSchema.getField( "col4" ), testData.get( 0 ) );
        AvroTypeConversion.convertMap( object.get(), Schema.createMap( Schema.create( Schema.Type.LONG ) ) );
    }

    @Test
    public void convertEnum_succ() throws Exception {
        Optional<Object> object =
                AvroTypeConversion.getGenRecFieldValue( writerSchema.getField( "col6" ), testData.get( 0 ) );
        Assert.assertNotNull( AvroTypeConversion.convertEnum( object.get(),
                Schema.createEnum( "Name", null, null, Arrays.asList( "A1", "b2", "c_3" ) ) ) );
        Schema schema = Schema.createEnum( "Name", null, null, Arrays.asList( "A1", "b2", "c_3" ) );
        Assert.assertThat( AvroTypeConversion.convertEnum( object.get(),
                schema ), is( new GenericData.EnumSymbol( schema, "c_3" ) ) );

    }

    @Test(
            expected = Exception.class )
    public void convertEnum_fail() throws Exception {
        Optional<Object> object =
                AvroTypeConversion.getGenRecFieldValue( writerSchema.getField( "col6" ), testData.get( 0 ) );
        Assert.assertNotNull( AvroTypeConversion.convertEnum( object.get(),
                Schema.createEnum( "Name", null, null, Arrays.asList( "NotExistsInData" ) ) ) );

    }

    @Test
    public void convertRecord_succ() throws Exception {
        Optional<Object> object =
                AvroTypeConversion.getGenRecFieldValue( writerSchema.getField( "col3" ), testData.get( 0 ) );
        Assert.assertNotNull(
                AvroTypeConversion.convertRecord( new GenericRecordBuilder( readerSchema.getField( "col3" ).schema() ),
                        readerSchema.getField( "col3" ).schema(), object.get() ) );
        GenericRecord data =
                AvroTypeConversion.convertRecord( new GenericRecordBuilder( readerSchema.getField( "col3" ).schema() ),
                        readerSchema.getField( "col3" ).schema(), object.get() );
        Assert.assertNotNull( data );
        Assert.assertThat( data.getSchema().toString(), is( readerSchema.getField( "col3" ).schema().toString() ) );
        ;
        Assert.assertThat( data.get( "inCol1" ).toString(), containsString( "innerRec" ) );
    }

    @Test(
            expected = Exception.class )
    public void convertRecord_failNotGenericRecord() throws Exception {
        AvroTypeConversion.convertRecord( new GenericRecordBuilder( readerSchema.getField( "col3" ).schema() ),
                readerSchema.getField( "col3" ).schema(), "not GenericRecord" );

    }

    @Test
    public void convertBytes_succ() throws Exception {
        byte[] bytesData = BigInteger.valueOf( 99 ).toByteArray();
        //Get bytes without transfomation
        Assert.assertNotNull(
                AvroTypeConversion.convertBytes( ByteBuffer.wrap( bytesData ), Schema.create( Schema.Type.BYTES ) ) );
        Assert.assertThat(
                AvroTypeConversion.convertBytes( ByteBuffer.wrap( bytesData ), Schema.create( Schema.Type.BYTES ) ),
                is( ByteBuffer.wrap( bytesData ) ) );

        //Convert from String
        Assert.assertNotNull(
                AvroTypeConversion.convertObj( Schema.create( Schema.Type.BYTES ), new Utf8( bytesData ) ) );
        Assert.assertThat( AvroTypeConversion.convertObj( Schema.create( Schema.Type.BYTES ), new Utf8( bytesData ) ),
                is( ByteBuffer.wrap( bytesData ) ) );

    }

    @Test(
            expected = Exception.class )
    public void convertRecord_fail() throws Exception {
        AvroTypeConversion.convertRecord( new GenericRecordBuilder( readerSchema.getField( "col3" ).schema() ),
                readerSchema.getField( "col3" ).schema(), "This is not valid GenericRecord" );
    }

    @Test
    public void convertObj_succ() throws Exception {
        Assert.assertNull( AvroTypeConversion.convertObj( Schema.create( Schema.Type.NULL ), "not reachable string" ) );

        byte[] bytesData = BigInteger.valueOf( 99 ).toByteArray();

        //Int
        Assert.assertNotNull( AvroTypeConversion.convertObj( Schema.create( Schema.Type.INT ), "1" ) );
        Assert.assertThat(
                AvroTypeConversion.convertObj( Schema.create( Schema.Type.INT ), "2147483647" ) instanceof Integer,
                is( true ) );
        Assert.assertThat( AvroTypeConversion.convertObj( Schema.create( Schema.Type.INT ), "1" ), is( 1 ) );
        // Long
        Assert.assertThat( AvroTypeConversion.convertObj( Schema.create( Schema.Type.LONG ), 1 ), is( 1L ) );
        Assert.assertThat( AvroTypeConversion.convertObj( Schema.create( Schema.Type.LONG ), "-777" ), is( -777L ) );
        // Float
        Assert.assertThat( AvroTypeConversion.convertObj( Schema.create( Schema.Type.FLOAT ), 1 ), is( 1F ) );
        Assert.assertThat( AvroTypeConversion.convertObj( Schema.create( Schema.Type.FLOAT ), "-777.0" ), is( -777F ) );
        // Double
        Assert.assertThat( AvroTypeConversion.convertObj( Schema.create( Schema.Type.DOUBLE ), 2147483648L ),
                is( 2147483648D ) );
        Assert.assertThat( AvroTypeConversion.convertObj( Schema.create( Schema.Type.DOUBLE ), "-777.0" ),
                is( -777D ) );
        // String
        Assert.assertThat( AvroTypeConversion.convertObj( Schema.create( Schema.Type.STRING ), new Utf8( "some" ) ),
                is( "some" ) );
        //Fixed
        Schema fixedSchema = Schema.createFixed( "Fixed", "doc", "", bytesData.length );
        Assert.assertThat(
                AvroTypeConversion.convertObj( fixedSchema, new GenericData.Fixed( fixedSchema, bytesData ) ),
                is( bytesData ) );

    }

    @Test(
            expected = Exception.class )
    public void convertObj_failNotUtf8() throws Exception {
        AvroTypeConversion.convertObj( Schema.create( Schema.Type.BYTES ), "not avro.Utf8" );
    }

    @Test(
            expected = Exception.class )
    public void convertObj_failEmphty() throws Exception {
        AvroTypeConversion.convertObj( Schema.create( Schema.Type.INT ), "" );
    }

    @Test(
            expected = Exception.class )
    public void convertObj_failNotParsableInt() throws Exception {
        AvroTypeConversion.convertObj( Schema.create( Schema.Type.INT ), "not parsable" );
    }

    @Test(
            expected = Exception.class )
    public void convertObj_failIntOutBound() throws Exception {
        AvroTypeConversion.convertObj( Schema.create( Schema.Type.INT ), "-2147483649" );
    }

    @Test(
            expected = Exception.class )
    public void convertObj_failNotLongNumber() throws Exception {
        AvroTypeConversion.convertObj( Schema.create( Schema.Type.LONG ), "1." );
    }

    @Test
    public void convert() throws Exception {

        GenericRecord o = AvroTypeConversion.create( testData.get( 0 ), readerSchema ).convert();
        Assert.assertThat( o.get( "notCol1" ), is( "record1" ) );
        Assert.assertThat( o.get( "notCol2" ), is( 1L ) );
        Assert.assertThat( o.get( "col3" ) instanceof GenericRecord, is( true ) );

        GenericRecord o2 = AvroTypeConversion
                .create( testData.get( 0 ),
                        Schema.createRecord( Arrays.asList(
                                new Schema.Field( "col7", Schema.create( Schema.Type.BYTES ), "doc", null ) ) ) )
                .convert();
        Assert.assertThat( o2.get( "col7" ), is( ByteBuffer.wrap( "test".getBytes() ) ) );
    }

    @Test(
            expected = Exception.class )
    public void convert_failNotGenericRecord() throws Exception {
        AvroTypeConversion.create( "not GenericRecord", readerSchema );
    }

    @Test(
            expected = Exception.class )
    public void convert_failNotRecord() throws Exception {
        AvroTypeConversion.create( testData.get( 0 ), Schema.create( Schema.Type.STRING ) );
    }

}
