package me.joniybek.avro;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AvroTypeConversion {

    private final GenericRecordBuilder rb;
    private final GenericRecord        source;
    private final Schema               targetSchema;

    public static AvroTypeConversion create( final Object sourceGenericRecord,
            final Schema targetSchema )
            throws AvroRuntimeException {
        if( targetSchema.getType() != Schema.Type.RECORD )
            throw new RuntimeException( "Head of avro schema hierarchy should be a RECORD!" );

        return new AvroTypeConversion( targetSchema, (GenericRecord) sourceGenericRecord );
    }

    private AvroTypeConversion( final Schema targetSchema, final GenericRecord source ) {
        this.targetSchema = targetSchema;
        this.source = source;
        this.rb = new GenericRecordBuilder( targetSchema );
    }

    public GenericRecord convert() {
        return convertRecord( rb, targetSchema, source );
    }

    static Object convertObj( final Schema fieldSchema, final Object obj ) {
        return convertObj( fieldSchema, obj, null, false );
    }

    static Object convertObj( final Schema fieldSchema, final Object obj, final Object defaultVal ) {
        return convertObj( fieldSchema, obj, defaultVal, false );
    }

    static Object convertObj( final Schema fieldSchema, final Object obj, final Object defaultVal,
            final boolean isNullable ) {
        if( obj == null ) {
            if( !isNullable && defaultVal == null )
                throw new RuntimeException( String.format( "Null at not nullable field and has no default value at: %s",
                        fieldSchema.getName() ) );
            else
                return defaultVal;
        }

        switch( fieldSchema.getType() ) {
            case NULL:
                return null;
            case BOOLEAN:
                return convertBool( obj, fieldSchema );
            case BYTES:
                return convertBytes( obj, fieldSchema );
            case INT:
                return tryCast( () -> Integer.valueOf( obj.toString() ), fieldSchema );
            case LONG:
                return tryCast( () -> Long.valueOf( obj.toString() ), fieldSchema );
            case FLOAT:
                return tryCast( () -> Float.valueOf( obj.toString() ), fieldSchema );
            case DOUBLE:
                return tryCast( () -> Double.valueOf( obj.toString() ), fieldSchema );
            case STRING:
                return tryCast( () -> String.valueOf( obj ), fieldSchema );
            case UNION:
                return convertUnion( obj, fieldSchema, isNullable );
            case ARRAY:
                return convertArray( obj, fieldSchema );
            case FIXED:
                return tryCast( ( (GenericData.Fixed) obj )::bytes, fieldSchema );//No Java alternative, nothing except Fixed can be converted
            case MAP:
                return convertMap( obj, fieldSchema );
            case ENUM:
                return convertEnum( obj, fieldSchema );
            case RECORD:
                return convertRecord( new GenericRecordBuilder( fieldSchema ), fieldSchema, obj );
            default:
                throw new AvroTypeException( String.format( "Cannot recognize field: %s  with type: %s",
                        fieldSchema.getName(), fieldSchema.getType() ) );
        }
    }

    static ByteBuffer convertBytes( final Object obj, final Schema fieldSchema ) {
        return tryCast( () -> {
            if( obj instanceof Utf8 )
                return ByteBuffer.wrap( ( (Utf8) obj ).getBytes() );
            else
                return ( (ByteBuffer) obj );
        }, fieldSchema );
    }

    static GenericData.EnumSymbol convertEnum( final Object obj, final Schema fieldSchema ) {
        return tryCast( () -> {
            final String value = String.valueOf( obj );
            if( fieldSchema.getEnumSymbols().contains( value ) ) {
                return new GenericData.EnumSymbol( fieldSchema, value );
            }
            throw new AvroTypeException( String.format( "Given string is not valid ENUM at field: %s, " +
                    "allowed values are: %s", fieldSchema.getName(),
                    Arrays.toString( fieldSchema.getEnumSymbols().toArray() ) ) );
        }, fieldSchema );
    }

    static Map convertMap( final Object obj, final Schema fieldSchema ) {
        return tryCast( () -> {
            final Map to = new HashMap();
            final Map from = (Map) obj;
            for( Object key : from.keySet() ) {
                to.put( key, convertObj( fieldSchema.getValueType(), from.get( key ) ) );
            }
            return to;
        }, fieldSchema );
    }

    static GenericData.Array convertArray( final Object obj, final Schema fieldSchema ) {
        return tryCast( () -> {
            final GenericData.Array from = (GenericData.Array) obj;
            final GenericData.Array<Object> to = new GenericData.Array<>( from.size(), fieldSchema );
            for( Object o : from ) {
                to.add( convertObj( fieldSchema.getElementType(), o ) );
            }
            return to;

        }, fieldSchema );
    }

    static Object convertUnion( final Object obj, final Schema fieldSchema, final boolean initiallyIsNullable ) {
        final List<Schema> fields = fieldSchema.getTypes();
        if( fields.stream().anyMatch( x -> x.getType() == Schema.Type.NULL ) ) {
            final List<Schema> remaining =
                    fields.stream().filter( x -> x.getType() != Schema.Type.NULL ).collect( Collectors.toList() );
            if( remaining.size() == 1 )
                return convertObj( remaining.get( 0 ), obj, null, true );
            else
                return convertObj( Schema.createUnion( remaining ), obj, null, true );

        } else {
            if( fields.stream()
                    .allMatch( x -> Arrays.asList( Schema.Type.INT, Schema.Type.LONG ).contains( x.getType() ) ) )
                return convertObj( Schema.create( Schema.Type.LONG ), obj, initiallyIsNullable );
            else if( fields.stream()
                    .allMatch( x -> Arrays.asList( Schema.Type.FLOAT, Schema.Type.DOUBLE ).contains( x.getType() ) ) ) {
                return convertObj( Schema.create( Schema.Type.DOUBLE ), obj, initiallyIsNullable );
            }
        }

        throw new UnsupportedOperationException( String.format( "No implementation for unions for field: %s  " +
                "with types: %s", fieldSchema.getName(), Arrays.toString( fieldSchema.getTypes().toArray() ) ) );
    }

    static GenericRecord convertRecord( final GenericRecordBuilder irb, final Schema schema,
            final Object innersource ) {
        if( !( innersource instanceof GenericRecord ) )
            throw new AvroTypeException( String.format( "Expected RECORD type" +
                    " got: %s ; at Schema field name: %s", innersource.getClass().getName(), schema.getName() ) );
        for( Schema.Field field : schema.getFields() ) {
            Optional node = getGenRecFieldValue( field, (GenericRecord) innersource );
            if( !node.isPresent() )
                throw new NoSuchElementException( String.format( "No such element named(alias): %s, " +
                        "alias:%s", field.name(), Arrays.toString( field.aliases().toArray() ) ) );
            irb.set( field.name(), convertObj( field.schema(), node.get(), field.defaultVal() ) );
        }
        return irb.build();

    }

    static Boolean convertBool( final Object value, final Schema schema ) {
        return tryCast( () -> {
            switch( value.toString().toUpperCase() ) {
                case "TRUE":
                    return Boolean.TRUE;
                case "FALSE":
                    return Boolean.FALSE;
                default:
                    throw new ClassCastException();
            }
        }, schema );
    }

    static <T> T tryCast( final Supplier<T> func, final Schema schema ) {
        try {
            return func.get();
        } catch( ClassCastException e ) {
            throw new ClassCastException(
                    String.format( "Cannot cast field: %s; to %s", schema.getName(), schema.getType() ) );
        }
    }

    public static Optional<Object> getGenRecFieldValue( final Schema.Field shouldExist,
            final GenericRecord mayContain ) {

        if( mayContain.getSchema().getField( shouldExist.name() ) != null ) {
            // Short-path
            return Optional.of( mayContain.get( shouldExist.name() ) );
        } else {
            // Match all field aliases+names for 'should-be' aliases+name
            final Optional<String> name = mayContain.getSchema().getFields().stream()
                    .filter( field -> shouldExist.aliases().contains( field.name() )
                            || field.aliases().stream()
                                    .anyMatch( in -> shouldExist.aliases().contains( in ) ) )
                    .findFirst().map( Schema.Field::name ); //TODO check if there can be 1+ fields with matching condition

            return name.isPresent() ? Optional.of( mayContain.get( name.get() ) ) : Optional.empty();
        }
    }
}
