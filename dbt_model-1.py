import json
import pandas
from snowflake.snowpark.functions import col, when, array_construct, array_position, lit, row_number
from snowflake.snowpark import functions as F
from snowflake.snowpark import Window, DataFrame

##################################### GET DATA FROM TABLES #####################################
def get_xref_table(dbt, domain):
    '''
    Description: Get data from xref table.
    Parameters:
        - dbt, a class compiled by dbt Core, enables you to run your Python code in your dbt project.
        - domain
    Return: A dataframe.
    '''
    xref_table = {
        "instrument":"bv_instrument_xref",
        "exchange":"bv_exchange_xref",
        "market":"bv_market_xref",
        "issuer":"bv_issuer_xref"
    }
    df_xref = dbt.ref("bv_instrument_xref")
    df_xref_is_active = df_xref.filter(col("IS_ACTIVE")==True)
    return df_xref_is_active

def get_mapping_table(dbt):
    '''
    Description: Get data from the column mapping table.
    Parameters:
        - dbt, a class compiled by dbt Core, enables you to run your Python code in your dbt project.
    Return: A dataframe.
    '''
    df_mapping  = dbt.ref("column_mapping")
    return df_mapping.to_pandas()

def get_priorities_by_column_table(dbt):
    '''
    Description: Get data from the column_survivorship_rules table.
    Parameters:
        - dbt, a class compiled by dbt Core, enables you to run your Python code in your dbt project.
    Return: A dataframe.
    '''
    df_column_priority  = dbt.ref("column_survivorship_rules")
    return df_column_priority.to_pandas()

def get_sources_priority_table(dbt):
    '''
    Description: Get data from the source_survivorship_rules table.
    Parameters:
        - dbt, a class compiled by dbt Core, enables you to run your Python code in your dbt project.
    Return: A dataframe.
    '''
    df_source_priority  = dbt.ref("source_survivorship_rules")
    return df_source_priority.to_pandas()

##################################### MANAGE DATA #####################################
def get_count_by_column(df, column):
    '''
    Description: Group by column and count the number of rows for each group, similar to a window function to count
    Parameters:
        - Dataframe: Corresponding Xref table
        - String: Domain
    Return: Dataframe
    '''
    
    # df['ID_COUNT'] = df.groupby(column)[column].transform('count')
    df = df.with_columns(
        F.count(column).over(Window().partition_by(column).order_by('LOAD_DATETIME')).alias("ID_COUNT")
    )
    return df

def get_columns_by_domain(df, domain):
    '''
    Description: Get the unique standard_name_key based on the domain using the column priority table
    Parameters:
        - Dataframe: Column priority table
        - String: Domain
    Return: List
    '''
    result_df = df[df['DOMAIN'] == domain]['STANDARD_NAME_KEY'].drop_duplicates()
    return result_df.tolist()

def get_priorities_by_column(df, domain, column):
    '''
    Description: Get a list of the sources priority by column and domain ordered by priority.
    Parameters:
        - Dataframe: Column priority table
        - String: Domain name
        - String: Column name
    Return: List
    '''
    filtered_df = df[(df['STANDARD_NAME_KEY'] == column) & (df['DOMAIN'] == domain)]
    return filtered_df.sort_values(by='SOURCE_PRIORITY')['SOURCE_KEY'].tolist()

def get_source_priorities_by_domain(df, domain):
    '''
    Description: Get the source priorities by domain ordered by priority.
    Parameters:
        - Dataframe: Column priority table
        - String: Domain name
    Return: List
    '''
    return df[df['DOMAIN'] == domain].sort_values(by='PRIORITY')['SOURCE_NAME'].tolist()

def assign_winner(group, priority_list):
    '''
    Description: For an specific group of rows with the same id, assign a winner based on the priority list.
    Parameters:
      - Dataframe: Group of rows with the same id but different sources
      - List: List of sources priority in order
    Return: Dataframe with the winner column among sources
    '''
    group['WINNER'] = 0
    group['PRIORITY'] = group['RECORD_SOURCE'].apply(lambda x: priority_list.index(x) if x in priority_list else float('inf'))
    highest_priority_index = group['PRIORITY'].idxmin()
    group.loc[highest_priority_index, 'WINNER'] = 1
    group.drop(columns='PRIORITY', inplace=True)
    only_winners_df = group[group['WINNER'] == 1].drop(columns=['WINNER'])
    return only_winners_df

def get_survivor_df(df_column_priority, non_mastered_df, df_ranked, domain, domain_id, columns):
    '''
    Description: Get a dataframe with the winners joined with the selected rows with source priority
    Parameters:
        - Dataframe: df_column_priority, contains the data from the column priority table
        - Dataframe: non_mastered_df, is the xref table with the non mastered rows.
        - Dataframe: df_ranked, is the xref table with the mastered rows.
        - String: Domain
        - String : Domain id related with the xref
        - List: List of columns to process
    Return: Dataframe
    '''
    for column_name in columns:
        if column_name.upper() in non_mastered_df.columns:
            temp_df = non_mastered_df[[domain_id, 'RECORD_SOURCE', column_name.upper()]].copy()
            priority_list = get_priorities_by_column(df_column_priority, domain, column_name)
            df_with_winner = temp_df.groupby(domain_id, group_keys=False).apply(lambda group: assign_winner(group, priority_list)).reset_index(drop=True)
            df_ranked = df_ranked.merge(df_with_winner[[domain_id, column_name.upper()]], on=domain_id, how='left', suffixes=('', '_extra'))
    return df_ranked

def get_survivor_df_snowpark(pd_column_priority, non_mastered_df:DataFrame, df_ranked:DataFrame, domain, domain_id, columns):
    '''
    Description: Get a dataframe with the winners joined with the selected rows with source priority
    Parameters:
        - DataFrame: df_column_priority, contains the data from the column priority table
        - DataFrame: non_mastered_df, is the xref table with the non-mastered rows.
        - DataFrame: df_ranked, is the xref table with the mastered rows.
        - String: Domain
        - String : Domain id related with the xref
        - List: List of columns to process
    Return: DataFrame
    ''' 
    for column_name in columns:
        if column_name.upper() in non_mastered_df.columns:
            temp_df = non_mastered_df.select(domain_id, 'RECORD_SOURCE', col(column_name.upper()))
            priority_list = get_priorities_by_column(pd_column_priority, domain, column_name)
            df_with_winner:DataFrame = assign_winner_snowpark(temp_df, priority_list)
            df_ranked = df_ranked.join(
                df_with_winner.select(domain_id, col(column_name.upper())),
                on=domain_id,
                how='left'
            )
    return df_ranked


def assign_winner_snowpark(group_df:DataFrame, priority_list, domain_id):
    # Create an array of priorities
    priority_array = array_construct(*priority_list)
    
    # Add PRIORITY column based on RECORD_SOURCE position in the priority_list
    group_df = group_df.with_column(
        "PRIORITY",
        when(
            col("RECORD_SOURCE").in_(priority_list),
            array_position(priority_array, col("RECORD_SOURCE")) - 1  # Adjusting for 0-based index
        ).otherwise(lit(99999999))  # Assign a large number for non-priority sources
    )
    
    # Define a window specification partitioned by domain_id, ordered by PRIORITY
    window_spec = Window.partition_by(col(domain_id)).order_by(col("PRIORITY"))
    
    # Assign WINNER flag: 1 for the row with the smallest PRIORITY in each group using row_number()
    group_df = group_df.with_column(
        "ROW_NUM",
        row_number().over(window_spec)
    )
    
    group_df = group_df.with_column(
        "WINNER",
        when(col("ROW_NUM") == 1, lit(1)).otherwise(lit(0))
    )
    
    # Filter winners and clean up columns
    only_winners_df = group_df.filter(col("WINNER") == 1).drop("PRIORITY", "ROW_NUM", "WINNER")
    
    return only_winners_df
def get_GR(dbt, domain_id, domain):
    #Call the xref table by domain
    xref_df = get_xref_table(dbt, domain)
    #Count by id, to process only the non unique rows
    xref_df:DataFrame = get_count_by_column(xref_df, domain_id)

    #Get mastered and mastered rows
    # non_mastered_df = xref_df[xref_df['ID_COUNT'] > 1].drop(columns=['ID_COUNT'])
    non_mastered_df = xref_df.filter(col('ID_COUNT')).drop('ID_COUNT')

    # mastered_df = xref_df[xref_df['ID_COUNT'] == 1].drop(columns='ID_COUNT')
    mastered_df = xref_df.filter(col('ID_COUNT')==1).drop('ID_COUNT')
    
    #Get the priorities from the source priority table, based on domain
    pd_source_priority = get_sources_priority_table(dbt)
    source_priorities = get_source_priorities_by_domain(pd_source_priority, domain)

    #Get the columns from the column priority table, based on domain
    pd_column_priority = get_priorities_by_column_table(dbt)
    columns = get_columns_by_domain(pd_column_priority, domain)
    upper_columns = [c.upper() for c in columns]

    #For each domain_id, get the corresponding winner
    # source_winners = non_mastered_df.groupby(domain_id, group_keys=False).apply(lambda group: assign_winner(group, source_priorities)).reset_index(drop=True)
    source_winners = assign_winner_snowpark(non_mastered_df, source_priorities, domain_id)
    # df_ranked = source_winners.copy().drop(columns=[col for col in upper_columns if col in source_winners.columns])
    df_ranked = source_winners.drop([col for col in upper_columns if col in source_winners.columns])

    # new_mastered = get_survivor_df(pd_column_priority, non_mastered_df, df_ranked, domain, domain_id, columns)
    new_mastered = get_survivor_df_snowpark(pd_column_priority, non_mastered_df, df_ranked, domain, domain_id, columns)

    # df_all_mastered = pandas.concat([new_mastered, mastered_df])
    df_all_mastered = new_mastered.union(mastered_df)

    return df_all_mastered

def model(dbt, session):
    '''
    Main function that creates a model and call all the logic.
    Return: A table.
    '''
    dbt.config(
        materialized = "table",
        python_version="3.11"
    )
    return get_GR(dbt, 'INSTRUMENT_ID', 'instrument')