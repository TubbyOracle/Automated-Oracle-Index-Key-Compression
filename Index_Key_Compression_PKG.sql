create or replace package Index_Key_Compression_pkg
  authid current_user
as

  --author      : Tyler Forsyth
  --date        : 19-NOV-2018
  --purpose     : Determine which indexes to compress based on repeating keys within index, compress indexes and report benefits
  --last tested : 11.2.0.3
  --              18.3.0.0
  --notes       : supports b-tree indexes
  --                partitioned     - indexes will be dropped and recreated
  --                non-partitioned - indexes will be rebuilt
  --            : does not support function based indexes (compressing these types of indexes is not advised based on experience)
  --            : would NOT run this on a production instance, ideal usage would be to run this on a copy of production,
  --                then analyze the results from the log table in order to determine which indexes to compress and the benefits
  --                the DDL require to apply the recommended changes will be found within the log table index_key_compression_log.new_index_DDL
  --                so we would want to extract that and plan an outage in production where we can apply these changes (ensure we have exclusive access to tables, etc)
  --DISCLAIMER  : if run in IMPLEMENT mode this code WILL modify indexes (drop partitioned indexes, rebuild non-partitioned indexes)
  --                the act of doing so could cause problems to existing applications, especially with questionably constructed SQL
  --

  --will control parallel degree for querying, as well as index rebuilding
  g_parallel_degree         number  :=  8;
  g_debug_enabled           boolean :=  true;

  --create below table in schema this code is compiled in, and the code
  --  will log metrics to it (it will delete data from it first based on the pi_schema_name parameter supplied
  --- to the code).

  /*
    drop table index_key_compression_log;

    create table index_key_compression_log
    (
        index_owner                 varchar2(128)
      , index_name                  varchar2(128)
      , index_modified              varchar2(1)       --will be Y when our recommendations are to add/modify key compression for index N when there is no advantage to using key compression (or changing existing key settings)
      , existing_prefix_compression number            --the existing key compression level (NULL if not utilized)
      , columns_compressed          number            --number of columns the code identified are optimal to utilize for key compression
      , original_leaf_blocks        number            --the number of leaf blocks that existed for the index before ANY modifications
      , before_leaf_blocks          number            --the number of leaf blocks for the existing index definition AFTER being rebuilt (could be there was just a bunch of wasted space in the index, so the code rebuilds it for a proper comparison of before/after key compression)
      , after_leaf_blocks           number            --the leaf blocks after the application of key compression for the specified number of columns
      , error_message               varchar2(4000)    --any error message encountered (table could be locked when we attempt to rebuild an index for example)
      , prior_index_DDL             clob              --the definition and re-create script for the index as it exists before the code makes any modifications
      , new_index_DDL               clob              --the DDL required to implement the recommended changes (only populated after running code in some form of IMPLEMENT mode
      , constraint index_key_compression_log_pk
          primary key (index_owner, index_name)
    );

  */

  --pi_schema_name              - if schema name is NULL then code will run for all schemas which are not maintained by oracle
  --pi_run_mode                 - valid values are :
  --                                ANALYZE
  --                                ANALYZE_IMPLEMENT
  --                                IMPLEMENT         --> requires a previous run with ANALYZE to have been specified
  --                                RETRY_ERRORS      --> will attempt to "redo" any indexes that failed according to the error_message column within index_key_compression_log
  --pi_restore_orig_index_def   - when TRUE will drop and recreate the original index
  procedure p_analyze_and_compress
  (
      pi_schema_name                all_indexes.owner%type    default null
    , pi_run_mode                   varchar2                  default 'ANALYZE'
    , pi_restore_orig_index_def     boolean                   default false
  );

end Index_Key_Compression_pkg;
/


create or replace package body Index_Key_Compression_pkg
as

  --global variable will be used, if the log table exists
  --  it will be used, otherwise old school dbms_outputs
  g_log_table_installed                 number;

--internal routines
  procedure p_debug_statement
  (
    in_message                          varchar2
  )
  is
  begin

    if
      g_debug_enabled
    then
      dbms_output.put_line(in_message);
    end if;--g_debug_enabled

  end p_debug_statement;


  --simple function to pad inputs with spaces for consistent dbms_outputs
  function f_pad_out_inputs
  (
      pi_input_string                   varchar2
    , pi_pad_length                     number  default 60
  )
  return
    varchar2
  is
  begin

    return rpad(pi_input_string, pi_pad_length, ' ');

  end f_pad_out_inputs;

  ---simple wrapper to call dbms_metadata to set transform parameters
  procedure p_set_meta_data_attribute
  (
      in_name                           varchar2
    , in_value                          boolean
  )
  is
  begin
    dbms_metadata.set_transform_param
    (
        transform_handle  =>  dbms_metadata.session_transform
      , name              =>  in_name
      , value             =>  in_value
    );
  end p_set_meta_data_attribute;


  --should work for partitioned as well as non-partitioned indexes
  function f_get_index_leaf_blocks
  (
      in_index_owner    varchar2
    , in_index_name     varchar2
  )
  return
    number
  is
    lf_leaf_blocks      number;
  begin

    select
      leaf_blocks
    into
      lf_leaf_blocks
    from
      all_indexes
    where
        owner       = in_index_owner
    and index_name  = trim(in_index_name);

    return lf_leaf_blocks;

  end f_get_index_leaf_blocks;


  --simple routine to log the results of key compression
  --  if the row exists perform an update (index owner/name are a unique key on the table)
  --  otherwise insert the row
  --We will also capture the existing index definition in case we need to use that to roll back our changes
  procedure  p_log_result
  (
      in_index_owner                    varchar2
    , in_index_name                     varchar2
    , in_existing_prefix_compression    number      default null
    , in_columns_compressed             number      default null
    , in_original_leaf_blocks           number      default null
    , in_before_leaf_blocks             number      default null
    , in_after_leaf_blocks              number      default null
    , in_error_message                  varchar2    default null
    , in_new_index_DDL                  varchar2    default null
    , in_index_modified                 varchar2    default null
  )
  is
    l_row_exists                        number(1);
    l_prior_index_DDL                   clob;
    l_original_leaf_blocks              number;
  begin

    select
      count(*)
    into
      l_row_exists
    from
      index_key_compression_log
    where--querying by primary key
        index_owner = in_index_owner
    and index_name  = in_index_name;

    if
      l_row_exists = 1
    then
      update
        index_key_compression_log
      set
          existing_prefix_compression = nvl(in_existing_prefix_compression  , existing_prefix_compression )
        , columns_compressed          = nvl(in_columns_compressed           , columns_compressed          )
        , original_leaf_blocks        = nvl(in_original_leaf_blocks         , original_leaf_blocks        )
        , before_leaf_blocks          = nvl(in_before_leaf_blocks           , before_leaf_blocks          )
        , after_leaf_blocks           = nvl(in_after_leaf_blocks            , after_leaf_blocks           )
        , new_index_DDL               = nvl(in_new_index_DDL                , new_index_DDL               )
        , index_modified              = nvl(in_index_modified               , index_modified              )
        , error_message               = in_error_message
      where
          index_owner =   in_index_owner
      and index_name  =   in_index_name;
    else -- need to insert a record

      --just in case, will be good to know what the DDL for the index looked like before we went mucking around with it
      --  this will be especially true for partitioned indexes
      l_prior_index_DDL   :=  DBMS_METADATA.get_ddl('INDEX', in_index_name, in_index_owner);

      if
        in_original_leaf_blocks is null
      then
        l_original_leaf_blocks  :=  f_get_index_leaf_blocks
                                    (
                                        in_index_owner  =>  in_index_owner
                                      , in_index_name   =>  in_index_name
                                    );
      else
        l_original_leaf_blocks  :=  in_original_leaf_blocks;
      end if;

      insert into index_key_compression_log
      (
          index_owner
        , index_name
        , existing_prefix_compression
        , columns_compressed
        , original_leaf_blocks
        , before_leaf_blocks
        , after_leaf_blocks
        , error_message
        , prior_index_DDL
        , new_index_DDL
        , index_modified
      )
      values
      (
          in_index_owner
        , in_index_name
        , in_existing_prefix_compression
        , in_columns_compressed
        , l_original_leaf_blocks
        , in_before_leaf_blocks
        , in_after_leaf_blocks
        , in_error_message
        , l_prior_index_DDL
        , in_new_index_DDL
        , nvl(in_index_modified, 'N')
      );
    end if;--l_row_exists = 1

  end p_log_result;

  --simple function to determine if the before/after compression is worth implementing
  function f_compression_is_favorable
  (
      in_leaf_blocks_BEFORE     number
    , in_leaf_blocks_AFTER      number
  )
  return
    boolean
  is
    l_return_value              boolean;
  begin

    p_debug_statement('before leaf blocks = ' || in_leaf_blocks_BEFORE || ' after leaf blocks = ' || in_leaf_blocks_AFTER);

    if
          in_leaf_blocks_AFTER >=  in_leaf_blocks_BEFORE              --index compression didn't work out for us (using more blocks after than before)
      or  round(in_leaf_blocks_AFTER/in_leaf_blocks_BEFORE*100) > 90  --expect better than a 10% return, less than that and the benefits of compression aren't likely worth the costs (added CPU to uncompress, etc)
      or  nvl(in_leaf_blocks_AFTER, 0) = 0
    then
      p_debug_statement('not favorable compression');
      l_return_value  :=  false;
    else
      p_debug_statement('favorable compression found');
      l_return_value  :=  true;
    end if;

    return l_return_value;

  end f_compression_is_favorable;


  --we cannot alter a partitioned index to add key compression
  --  so we will need to drop the index and create it with the key compression
  --  as part of the definition
  procedure p_compress_partitioned_index
  (
      in_index_owner                    varchar2
    , in_index_name                     varchar2
    , in_existing_prefix_compression    number
    , in_columns_compressed             number
    , in_original_leaf_blocks           number    default null
    , in_root_call                      boolean   default true
  )
  is
    l_qualified_index_name              varchar2(500)   :=  in_index_owner || '.' || in_index_name;
    l_noparallel                        varchar2(4000)  :=  'alter index  ' || l_qualified_index_name || ' noparallel';
    l_drop_index                        varchar2(4000)  :=  'drop index   ' || l_qualified_index_name;
    l_leaf_blocks_BEFORE                all_indexes.leaf_blocks%type;
    l_leaf_blocks_AFTER                 all_indexes.leaf_blocks%type;
    l_original_index_DDL                clob;
    l_new_index_DDL                     clob;
    l_protected_index                   number(1);
  begin

    p_debug_statement('Processing Compression for partitioned index = ' || l_qualified_index_name || ' for column number = ' || in_columns_compressed);

    --if the index is unique and there's a constraint being enforced by it we won't be able to drop it.
    --  don't want the code getting cheeky with disabling constraints and things like that so we're just going to
    --  call it a day and not process it
    select
      count(*)
    into
      l_protected_index
    from
      dual
    where exists
    (
      select
        null
      from
        all_constraints
      where
          index_owner     = in_index_owner
      and index_name      = in_index_name
      and constraint_type in ('U', 'P')
      and status          = 'ENABLED'   --if the constraint is disabled we'll proceed with key compression if we can
    );

    if
      l_protected_index = 1
    then
      p_debug_statement('index in place to support unique/primary key constraint, will not be able to drop so it will be skipped');

      return;
    end if;



    -- we need to ensure that we capture the index definition before we start dropping the index
    if
      in_root_call
    then
      l_leaf_blocks_BEFORE :=  f_get_index_leaf_blocks(in_index_owner, in_index_name);

      p_log_result
      (
          in_index_owner                  =>  in_index_owner
        , in_index_name                   =>  in_index_name
        , in_existing_prefix_compression  =>  in_existing_prefix_compression
        , in_columns_compressed           =>  in_columns_compressed
        , in_original_leaf_blocks         =>  in_original_leaf_blocks
        , in_before_leaf_blocks           =>  l_leaf_blocks_BEFORE
      );
    end if;

    select
      prior_index_DDL
    into
      l_original_index_DDL
    from
      index_key_compression_log
    where
        index_owner = in_index_owner
    and index_name  = in_index_name;

    if
      in_existing_prefix_compression is not null
    then
      --we need to adjust the DDL to remove any compression definition
      l_new_index_DDL :=  substr(l_original_index_DDL, 1, instr(l_original_index_DDL, ')', -1) );
    else
      l_new_index_DDL :=  l_original_index_DDL;
    end if; --in_existing_prefix_compression is not null


    --drop index
    execute immediate l_drop_index;
    --add key compress to index definition
    l_new_index_DDL :=  l_new_index_DDL || ' compress ' || in_columns_compressed || ' parallel ' || g_parallel_degree;
    execute immediate l_new_index_DDL ;
    --remove parallel definition
    execute immediate l_noparallel;

    l_leaf_blocks_AFTER :=  f_get_index_leaf_blocks(in_index_owner, in_index_name);

    if
      f_compression_is_favorable
      (
          in_leaf_blocks_BEFORE   =>  l_leaf_blocks_BEFORE
        , in_leaf_blocks_AFTER    =>  l_leaf_blocks_AFTER
      )
    then
      p_log_result
      (
          in_index_owner                  =>  in_index_owner
        , in_index_name                   =>  in_index_name
        , in_existing_prefix_compression  =>  in_existing_prefix_compression
        , in_columns_compressed           =>  in_columns_compressed
        , in_before_leaf_blocks           =>  l_leaf_blocks_BEFORE
        , in_after_leaf_blocks            =>  l_leaf_blocks_AFTER
        , in_new_index_DDL                =>  l_new_index_DDL
        , in_index_modified               =>  'Y'
      );
    else
        --try again recursively, first drop the index
      execute immediate l_drop_index;
      --recreate the index
      execute immediate l_original_index_DDL || ' parallel ' || g_parallel_degree;
      --remove parallel definition
      execute immediate l_noparallel;

      if
        in_columns_compressed = 1
      then
        p_debug_statement('exhausted all possible column permutations, no index modifications to retain (original definition restored)');
      else
        p_debug_statement('attempting with lower number of columns for compression');

        p_compress_partitioned_index
        (
            in_index_owner                  =>  in_index_owner
          , in_index_name                   =>  in_index_name
          , in_existing_prefix_compression  =>  in_existing_prefix_compression
          , in_columns_compressed           =>  in_columns_compressed - 1
          , in_original_leaf_blocks         =>  in_original_leaf_blocks
          , in_root_call                    =>  false
        );
      end if;--incolumn_number = 1

    end if;--f_compression_is_favorable

  end p_compress_partitioned_index;


  --for a non-partitioned index (regular b-tree) we can simply alter the index to add
  --  the index key compression attribute
  procedure p_compress_regular_index
  (
      in_index_owner                    varchar2
    , in_index_name                     varchar2
    , in_existing_prefix_compression    number
    , in_columns_compressed             number
    , in_original_leaf_blocks           number    default null
    , in_root_call                      boolean   default true
    , in_restore_original_DDL           varchar2  default null
  )
  is
    l_qualified_index_name              varchar2(500)   :=  in_index_owner || '.' || in_index_name;
    l_noparallel                        varchar2(4000)  :=  'alter index  ' || l_qualified_index_name || ' noparallel';
    l_new_index_DDL                     clob            :=  'alter index  ' || l_qualified_index_name || ' rebuild parallel ' || g_parallel_degree || ' compress  [column_position]';
    l_leaf_blocks_BEFORE                all_indexes.leaf_blocks%type;
    l_leaf_blocks_AFTER                 all_indexes.leaf_blocks%type;
    l_total_columns_in_index            number;
    l_compress_semantics                varchar2(100);
    l_restore_original_DDL              varchar2(4000);
  begin

    p_debug_statement('Processing Compression for regular index = ' || l_qualified_index_name || ' for column number = ' || in_columns_compressed);

    select
      count(*)
    into
      l_total_columns_in_index
    from
      all_ind_columns
    where
        index_owner =   in_index_owner
    and index_name  =   in_index_name;

    l_compress_semantics  :=  case when in_columns_compressed = 1 or in_columns_compressed = l_total_columns_in_index then null else in_columns_compressed end;

    --original leaf blocks will be the number of blocks in the index before this program touches it.
    --  good number to have, since rebuilding the index may drastically change this number (lots of deletes done, index never rebuilt for example).
    --this will always be null for "root calls", if we call this routine recursively we'll have a non-null value (passed from the root call)
    if
      in_root_call
    then
      --could be the existing index is just really fat because of no prior maintenance, good to know that, but also good to start from a "fresh" perspective
      --  so we will rebuild the index (no compression) since comparing to that number makes more sense when evaluating the benefits of the key compression
      l_restore_original_DDL  :=  'alter index '  ||  l_qualified_index_name || ' rebuild parallel ' || g_parallel_degree;
      if
        in_existing_prefix_compression is not null
      then
        l_restore_original_DDL  :=  l_restore_original_DDL  ||  ' compress ' || l_compress_semantics;
      end if;

      p_debug_statement('SQL #1 = ' || l_restore_original_DDL);

      execute immediate l_restore_original_DDL;
      execute immediate l_noparallel;
    else
      l_restore_original_DDL  :=  in_restore_original_DDL;
    end if;

    l_leaf_blocks_BEFORE :=  f_get_index_leaf_blocks(in_index_owner, in_index_name);

    l_new_index_DDL :=  replace(l_new_index_DDL, '[column_position]', l_compress_semantics);
    p_debug_statement('SQL #2 = ' || l_new_index_DDL);

    execute immediate l_new_index_DDL;
    execute immediate l_noparallel;

    l_leaf_blocks_AFTER :=  f_get_index_leaf_blocks(in_index_owner, in_index_name);

    if
      f_compression_is_favorable
      (
          in_leaf_blocks_BEFORE   => l_leaf_blocks_BEFORE
        , in_leaf_blocks_AFTER    => l_leaf_blocks_AFTER
      )
    then
      p_log_result
      (
          in_index_owner                  =>  in_index_owner
        , in_index_name                   =>  in_index_name
        , in_existing_prefix_compression  =>  in_existing_prefix_compression
        , in_columns_compressed           =>  in_columns_compressed
        , in_before_leaf_blocks           =>  l_leaf_blocks_BEFORE
        , in_after_leaf_blocks            =>  l_leaf_blocks_AFTER
        , in_new_index_DDL                =>  l_new_index_DDL
        , in_index_modified               =>  'Y'
      );
    else--not a happy outcome so revert (uncompress) the index

      p_debug_statement('SQL #3 = ' || l_restore_original_DDL);

      --restore index (uncompress it)
      execute immediate l_restore_original_DDL;
      execute immediate l_noparallel;

      if
        in_columns_compressed = 1
      then
        p_debug_statement('exhausted all possible column permutations, no index modifications to retain (original definition restored)');
      else
        p_debug_statement('attempting with lower number of columns for compression');

        --try with less columns and see how we go
        p_compress_regular_index
        (
            in_index_owner                  =>  in_index_owner
          , in_index_name                   =>  in_index_name
          , in_existing_prefix_compression  =>  in_existing_prefix_compression
          , in_columns_compressed           =>  in_columns_compressed  - 1
          , in_original_leaf_blocks         =>  in_original_leaf_blocks
          , in_root_call                    =>  false
          , in_restore_original_DDL         =>  l_restore_original_DDL
        );
      end if; --in_columns_compressed =  1

    end if;--f_compression_is_favorable

  exception
    when others
      then

        p_debug_statement('when others encountered attempting SQL');

        p_log_result
        (
            in_index_owner                  =>  in_index_owner
          , in_index_name                   =>  in_index_name
          , in_existing_prefix_compression  =>  in_existing_prefix_compression
          , in_columns_compressed           =>  in_columns_compressed
          , in_before_leaf_blocks           =>  l_leaf_blocks_BEFORE
          , in_after_leaf_blocks            =>  l_leaf_blocks_AFTER
          , in_error_message                =>  sqlcode || '-' || sqlerrm
        );

  end p_compress_regular_index;


  --This procedure will compress the index to the specified level (in_columns_compressed) and then evaluate the results (after blocks relative to the before blocks)
  --  if the code determines that the index should not have been compressed adn the level does not equal 1 then the code will uncompress the index and
  --  recursively call itself with level - 1 to see if compressing less columns within the index yeilds a benefit.
  --At the end of the routine, if the index compression has been beneficial we will dbms_output the information
  --  if no compression is done no output will be performed, if the code raises an error (index locked and cannot acquire a lock for example) that error message
  --  will also be output
  procedure p_compress_index
  (
      in_index_owner                    varchar2
    , in_index_name                     varchar2
    , in_existing_prefix_compression    number
    , in_columns_compressed             number
    , in_implement_recommendations      boolean default false
    , in_partitioned_index              boolean
  )
  is
    l_original_leaf_blocks              number;
  begin

    --first step will be to log this index in our tracking table, this will ensure
    --  we capture the DDL of the existing index before any changes are done
    p_log_result
    (
        in_index_owner                  =>  in_index_owner
      , in_index_name                   =>  in_index_name
      , in_existing_prefix_compression  =>  in_existing_prefix_compression
      , in_columns_compressed           =>  in_columns_compressed
    );

    --this value gets populated by the above log results procedure call
    select
      original_leaf_blocks
    into
      l_original_leaf_blocks
    from
      index_key_compression_log
    where
        index_owner = in_index_owner
    and index_name  = in_index_name;

    if
      in_implement_recommendations
    then

      if
        in_partitioned_index
      then
        p_debug_statement('running partitioned index compression');

        p_compress_partitioned_index
        (
            in_index_owner                    =>  in_index_owner
          , in_index_name                     =>  in_index_name
          , in_existing_prefix_compression    =>  in_existing_prefix_compression
          , in_columns_compressed             =>  in_columns_compressed
          , in_original_leaf_blocks           =>  l_original_leaf_blocks
        );

      else
        p_debug_statement('running NON-partitioned index compression');

        p_compress_regular_index
        (
            in_index_owner                    =>  in_index_owner
          , in_index_name                     =>  in_index_name
          , in_existing_prefix_compression    =>  in_existing_prefix_compression
          , in_columns_compressed             =>  in_columns_compressed
          , in_original_leaf_blocks           =>  l_original_leaf_blocks
        );
      end if;--in_partitioned_index

    end if;--in_implement_recommendations

  end p_compress_index;


  --procedure will be used when the code is run in ANALYZE only mode first, which will
  --  populate the index_key_compression_log table, so we will drive off of that table
  --  and call our routines from there
  procedure p_implement_recommendations
  (
    pi_schema_name        all_indexes.owner%type    default null
  )
  is
  begin

    p_debug_statement('implementing recommendations');

    for idx in
    (
      select
          cl.index_owner            as  index_owner
        , cl.index_name             as  index_name
        , cl.columns_compressed     as  columns_compressed
        , (
            select
              ai.partitioned
            from
              all_indexes ai
            where
                ai.owner      = cl.index_owner
            and ai.index_name = cl.index_name
          )
                                    as  partitioned
      from
        index_key_compression_log   cl
      where
          cl.index_owner  = pi_schema_name
      or  pi_schema_name  is null
      order by
          cl.index_owner  asc
        , cl.index_name   asc
    )
    loop

      p_debug_statement('*********************processing new index ' || f_pad_out_inputs(idx.index_owner || '.' || idx.index_name) || '*********************' );

      p_compress_index
      (
          in_index_owner                  =>  idx.index_owner
        , in_index_name                   =>  idx.index_name
        , in_existing_prefix_compression  =>  null
        , in_columns_compressed           =>  idx.columns_compressed
        , in_implement_recommendations    =>  true
        , in_partitioned_index            =>  case when idx.partitioned = 'YES' then true else false end
      );
    end loop idx;

  end p_implement_recommendations;


  --code will both analyze indexes for key compression and then proceed to implement the recommendations
  --if the parameter pi_analyze_only is true then the implementation will not happen, analyze only
  procedure p_analyze_and_implement
  (
      pi_schema_name        all_indexes.owner%type
    , pi_analyze_only       boolean
  )
  is
    l_average_count         number;
    l_column_list           varchar2(4000);
    l_last_column_position  number;
    l_username_list         sys.odcivarchar2list  :=  sys.odcivarchar2list();
  begin

    p_debug_statement('analyzing and implementing recommendations (' || case when pi_analyze_only then 'TRUE' else 'FALSE' end || ')' );

    --if no schema name was specified get a list of non-oracle maintained users
    if
      pi_schema_name is null
    then
      begin
        execute immediate q'!
                              select
                                au.username
                              from
                                all_users au
                              where
                                au.oracle_maintained = 'N'    --column doesn't exist prior to 12
                            !'
                            bulk collect into l_username_list;
      exception
        when others then
          raise_application_error(-20101, 'NULL schema lists are only compatible with version 12 and later, please specify a username');
      end;--exception catch
    else
      l_username_list :=  sys.odcivarchar2list(pi_schema_name);
    end if;

    p_debug_statement('processing schema = ' || pi_schema_name);

    for idx in
    (
      select
          ui.index_name
        , ui.table_name
        , ui.owner
        , ui.partitioned
        , ui.prefix_length
      from
        all_indexes ui
      where
          ui.owner      in  (
                              select--cardinality (mem, 1)
                                mem.column_value
                              from
                                table(cast(l_username_list as sys.odcivarchar2list))  mem
                            )
      and ui.table_name !=  upper('index_key_compression_log')  --ignore log table for this process if installed
      and ui.index_type =   'NORMAL'                            --don't want function based, IOT or bitmap indexes
      and not                                                   --ignore single column unique indexes
      (
            ui.uniqueness   = 'UNIQUE'
        and 1               = (
                                select
                                  count(*)
                                from
                                  all_ind_columns uic
                                where
                                    uic.index_owner = ui.owner
                                and uic.index_name  = ui.index_name
                              )
      )
      and not exists
      (
        select
          null                                                  --skip any funtion based indexes (any column has a function applied to it)
        from
          all_ind_expressions uie
        where
            uie.index_owner = ui.owner
        and uie.index_name  = ui.index_name
      )
      and not exists                                            --ignore temp tables (GTTs) and index organized tables (IOTS)
      (
        select
          null
        from
          all_tables ut
        where
            ut.owner      = ui.owner
        and ut.table_name = ui.table_name
        and
        (
              ut.temporary  = 'Y'       --exclude GTT
          or  ut.iot_type is not null   --exclude IOT
        )
      )
      order by
          ui.owner      asc
        , ui.table_name asc
        , ui.index_name asc
    )
    loop
      p_debug_statement('*********************processing new index ' || f_pad_out_inputs(idx.owner || '.' || idx.index_name) || '*********************' );

      l_column_list           :=  null;
      l_last_column_position  :=  null;

      --we want to start with the first column of the index, check the number of repeated keys for that, if greater than our metric
      --  we will move on to the next column in the index and check for the combination of COL1, COL2, and so on until we have exhausted all
      --  columns in the index or our average count is below our predefined threshold.
      for det_idx in
      (
        select
            ui.column_name      as column_name
          , ui.column_position  as column_position
        from
          all_ind_columns ui
        where
            ui.index_owner  = idx.owner
        and ui.index_name   = idx.index_name
        order by
          ui.column_position asc
      )
      loop
        l_column_list := ltrim(l_column_list || ',' || det_idx.column_name, ',');

        execute immediate '
                            select
                              avg(the_count_ah_ha_ha)
                            from
                            (
                              select --+ parallel (' || g_parallel_degree || ')
                                count(*) as the_count_ah_ha_ha
                              from      ' || idx.owner || '.' || idx.table_name || '
                              group by  ' || l_column_list  || '
                            )
                          '
                          into l_average_count;

        if
          l_average_count < 5 --arbitrary number I chose, seems like less than 5 and compression isn't worth it
        then
          --when we hit a condition whereby we don't think compression is an advantage we want to exit the loop (det_idx) because adding more columns to the group by isn't going to improve our situation
          --  and we will use the last column position as our go to for the number of columns to compress
          exit;
        else
          l_last_column_position  :=  det_idx.column_position;
        end if;

      end loop det_idx;

      p_debug_statement('Optimal columns to Compress = ' || l_last_column_position);

      if
        l_last_column_position is not null
      then

        p_log_result
        (
            in_index_owner                  =>  idx.owner
          , in_index_name                   =>  idx.index_name
          , in_existing_prefix_compression  =>  idx.prefix_length
          , in_columns_compressed           =>  l_last_column_position
        );

        if
          pi_analyze_only
        then
          null;
        else
          p_compress_index
          (
              in_index_owner                  =>  idx.owner
            , in_index_name                   =>  idx.index_name
            , in_existing_prefix_compression  =>  idx.prefix_length
            , in_columns_compressed           =>  l_last_column_position
            , in_implement_recommendations    =>  case when pi_analyze_only         then false  else true   end
            , in_partitioned_index            =>  case when idx.partitioned = 'YES' then true   else false  end
          );
        end if;--pi_analyze_only

      end if;--l_last_column_position is not null

    end loop idx;

  end p_analyze_and_implement;


  --loop through control table for all indexes that encountered an error and retry that index
  procedure p_retry_errors
  (
    pi_schema_name        all_indexes.owner%type
  )
  is
  begin

    for retry_idx in
    (
      select
          key_log.index_owner
        , key_log.index_name
        , key_log.existing_prefix_compression
        , key_log.columns_compressed
        , all_ind.partitioned
      from
          index_key_compression_log   key_log
        , all_indexes                 all_ind
      where
          (key_log.index_owner = pi_schema_name or pi_schema_name is null)
      and key_log.error_message is not null
      and key_log.index_owner = all_ind.owner
      and key_log.index_name  = all_ind.index_name
    )
    loop

      p_debug_statement('retrying index - ' || retry_idx.index_owner || '.' || retry_idx.index_name);

      p_compress_index
      (
          in_index_owner                  =>  retry_idx.index_owner
        , in_index_name                   =>  retry_idx.index_name
        , in_existing_prefix_compression  =>  retry_idx.existing_prefix_compression
        , in_columns_compressed           =>  retry_idx.columns_compressed
        , in_implement_recommendations    =>  true
        , in_partitioned_index            =>  case when retry_idx.partitioned = 'YES' then true   else false  end
      );

    end loop retry_idx;

  end p_retry_errors;


  procedure p_restore_orig_indexes
  is
    l_sql     clob;
  begin

    for all_changed_indexes_idx in
    (
      select
          key_log.index_owner || '.' || key_log.index_name  as  index_name
        , key_log.prior_index_ddl                           as  prior_index_ddl
      from
        index_key_compression_log   key_log
      where
          key_log.index_modified  = 'Y'
      and not exists
      ( --won't be able to drop unique/primary keys
        select
          null
        from
          all_indexes all_idx
        where
            all_idx.owner       = key_log.index_owner
        and all_idx.index_name  = key_log.index_name
        and all_idx.uniqueness  = 'UNIQUE'
      )
    )
    loop

      p_debug_statement('restoring index - ' || all_changed_indexes_idx.index_name );

      l_sql     :=  'drop index ' || all_changed_indexes_idx.index_name;
      execute immediate l_sql;

      l_sql     :=  all_changed_indexes_idx.prior_index_ddl;
      execute immediate l_sql;

    end loop all_changed_indexes_idx;

  exception
    when others
    then
      raise_application_error(-20101, 'failed on sql = ' || l_sql || ' - aborting program error = ' || sqlcode || '-' || sqlerrm);

  end p_restore_orig_indexes;

--published routines

  procedure p_analyze_and_compress
  (
      pi_schema_name                all_indexes.owner%type    default null
    , pi_run_mode                   varchar2                  default 'ANALYZE'
    , pi_restore_orig_index_def     boolean                   default false
  )
  is
    l_recommendations_exist number;
  begin

    if
          pi_run_mode not in ('ANALYZE', 'ANALYZE_IMPLEMENT', 'IMPLEMENT', 'RETRY_ERRORS')
      or  pi_run_mode is null
    then
      raise_application_error(-20101, 'Unsupported value for parameter pi_run_mode');
    end if;

    if
      pi_run_mode = 'IMPLEMENT'
    then

      select
        count(*)
      into
        l_recommendations_exist
      from
        dual
      where
        exists
      (
        select
          null
        from
          index_key_compression_log
        where
            index_owner = pi_schema_name
        or pi_schema_name is null
      );

      if
        l_recommendations_exist = 1
      then
        p_implement_recommendations
        (
          pi_schema_name  =>  pi_schema_name
        );
      else
        raise_application_error(-20101,'no recommendations exist, please run code in analyze mode before attempting to implement recommendations');
      end if;--l_recommendations_exist != 0

    elsif
      pi_run_mode = 'RETRY_ERRORS'
    then

      p_retry_errors
      (
        pi_schema_name    =>  pi_schema_name
      );

    else--not pi_implement_analysis so we will do both an analysis and implementation

      --clear out log table if exists
      delete
        index_key_compression_log
      where
          index_owner = pi_schema_name
      or pi_schema_name is null;

      p_analyze_and_implement
      (
          pi_schema_name    =>  pi_schema_name
        , pi_analyze_only   =>  case when pi_run_mode = 'ANALYZE' then true else false end
      );

    end if;--pi_implement_analysis

    commit;

    --if enabled drop index and create with original definition
    if
      pi_restore_orig_index_def
    then
      p_restore_orig_indexes;
    end if;

  end p_analyze_and_compress;


begin--package initialization

  --we're going to want to extract the current index definition and store that, will be good to have a "backing out" plan if needed
  p_set_meta_data_attribute('SQLTERMINATOR'       , false);
  p_set_meta_data_attribute('PRETTY'              , true);
  p_set_meta_data_attribute('SEGMENT_ATTRIBUTES'  , true);
  p_set_meta_data_attribute('STORAGE'             , true);

end Index_Key_Compression_pkg;
/
