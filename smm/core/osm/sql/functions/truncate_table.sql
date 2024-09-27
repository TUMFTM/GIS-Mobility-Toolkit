SET search_path TO $schema;
DO $$                  
    BEGIN 
        IF EXISTS
            ( SELECT 1
            FROM   information_schema.tables 
            WHERE  table_schema = '$schema'
            AND    table_name = '$table'
            )
        THEN
            TRUNCATE $table RESTART IDENTITY;
        END IF ;
    END
$$ ;