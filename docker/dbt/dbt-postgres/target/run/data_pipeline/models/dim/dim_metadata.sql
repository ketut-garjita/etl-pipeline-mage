
  
  create view "warehouse"."main"."dim_metadata__dbt_tmp" as (
    

select
    -- attributes
    
    
    cast(key as TEXT)
 as metadata_key,
    
    
    cast(metadata_offset as integer)
 as metadata_offset,
    
    
    cast(partition as integer)
 as metadata_partition,
    
    
    cast(time as integer)
 as metadata_time,
    
    
    cast(topic as TEXT)
 as metadata_topic

from "warehouse"."main"."stg_metadata"
  );
