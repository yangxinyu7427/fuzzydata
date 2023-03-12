import itertools
import os
import string
from collections import defaultdict

import pandas
import numpy as np
import logging

from functools import partial
from typing import Callable, Dict, List

import pandas as pd
from faker import Faker
from itertools import chain


logging.getLogger('faker').setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_UNIQUE_DICTIONARY = string.ascii_letters+string.digits


def load_function_dict(directory=_THIS_DIR+'/config/'):
    return {
        'joinable': [line.rstrip('\n') for line in open(directory + 'joinable_cols.txt')],
        'groupable': [line.rstrip('\n') for line in open(directory + 'groupable_cols.txt')],
        'numeric': [line.rstrip('\n') for line in open(directory + 'numeric_cols.txt')],
        'string': [line.rstrip('\n') for line in open(directory + 'string_cols.txt')],
    }


def generate_inverse_function_dict(function_dict):
    inv_functions = defaultdict(list)
    for k, vs in function_dict.items():
        for v in vs:
            inv_functions[v].append(k)
    return inv_functions


_gen_functions = load_function_dict()
logger.debug(_gen_functions)
_faker_cols = list(set(chain(*_gen_functions.values())))
_inv_gen_functions = generate_inverse_function_dict(_gen_functions)


def generate_prefix(symbol_dict: str, size: int=5) -> str:
    return ''.join(np.random.choice(list(symbol_dict), size))


def generate_table(num_rows: int=100, column_dict: Dict=None, pd=pandas, key_series=None) -> pandas.DataFrame:
    """
    Generate a table with a given schema and number of rows
    :param num_rows: Number of rows desired in the table
    :param column_dict: Schema Mapping (column_label->faker_provider) as a Dict
    :param pd: pandas library to be used to generated (default pandas), you can also use modin.pandas
    :param key_series: A pd.Series object that contains a key column to be left-appended to the df. Overrides num_rows.
    :return: Dataframe with generated table according to spec.
    """
    faker = Faker()

    series_list = []
    label_list = []

    if key_series is not None:
        series_list.append(key_series)
        label_list.append(key_series.name)
        logger.info(f'Generating right-merge df df with {num_rows} rows and {len(column_dict.keys())} columns')
    else:
        logger.info(f'Generating base df with {num_rows} rows and {len(column_dict.keys())} columns')

    for label, column in column_dict.items():
        series_list.append(pd.Series((faker.format(column) for _ in range(num_rows))))
        label_list.append(label)

    logger.debug(f'Column list: {label_list}')
    return pd.concat(series_list, axis=1, keys=label_list)


def generate_schema(num_cols: int, unique_prefix: Callable = partial(generate_prefix, _UNIQUE_DICTIONARY, size=5)) -> Dict[str, str]:
    """
    Generates a randomized schema given number of columns.
    :param num_cols: Number of columns to generate.
    :param unique_prefix: A function that generates a unique column prefix (default is 5 char random string).
    :return: Dict of column_label->faker provider as per spec.
    """
    column_dict = {}
    num_col_types = len(_gen_functions.keys())
    if num_cols < num_col_types:
        random_selection = np.random.choice(_faker_cols, size=num_cols)
    else:
        # Better randomization of columns to ensure at least one of each type are generated
        random_selection = []
        num_array = np.ones(num_col_types, dtype=int)
        while sum(num_array) < num_cols:
            ix = np.random.randint(0, 4)
            num_array[ix] += 1
        for ix, col_type in enumerate(_gen_functions.keys()):
            random_selection.extend(np.random.choice(_gen_functions[col_type], size=num_array[ix]))

    logger.debug(random_selection)
    column_dict.update({f'{unique_prefix()}__{r}': r for r in random_selection})
    logger.debug(f'Selected columns for this schema: {column_dict.values()}')
    return column_dict


def get_schema_type_mapping(column_dict):
    # Do not need inverse schema maps yet...
    schema_type_mapping = defaultdict(list)
    for col, faker_type in column_dict.items():
        for col_type in _inv_gen_functions[faker_type]:
            schema_type_mapping[col_type].append(col)

    logger.debug(f'Inverse ColumnType Mapping: {schema_type_mapping}')

    return schema_type_mapping


def select_rand_cols(df_col_types, num, col_type=None):
    """
    Select a random "num" of columns from a given column_name: type mapping
    :param df_col_types: Mapping of column names to types.
    :param num: Number of columns required
    :param col_type: Column types required
    :return:
    """
    if not col_type:
        all_options = list(itertools.chain(df_col_types.values()))
    else:
        all_options = df_col_types[col_type]
    try:
        logger.debug(f'Selection Options for {col_type} type: {all_options}')
        options = np.random.choice(all_options, num, replace=False).tolist()
    except ValueError as e:
        logger.warning(f'Could not select {num} columns of type {col_type}')
        return None
    return options


def select_rand_aggregate():
    return np.random.choice(['min', 'max', 'sum', 'mean', 'count'], 1)[0]


def get_rand_percentage(minimum=0.1, maximum=0.99):
    return round((maximum - minimum) * np.random.random_sample() + minimum,  2)


def generate_pkfk_join_table(source_table, source_schema: Dict['str', 'str'],
                             key_col: str, new_col_size=None, pd=pandas):
    """
    Generates a randomized PK-FK table (right table) for a merge/join operation, given a source schema and key_column.
    :param source_table: Source table to be joined.
    :param source_schema: Source Schema.
    :param key_col: Column Label to be used as a key.
    :param new_col_size: Number of columns required for the new table .
    :param pd: pandas library to be used.
    :return:
    """
    key_values = list(set(source_table[key_col].values))
    key_series = pd.Series(data=key_values, name=key_col)
    if not new_col_size:
        new_col_size = np.random.randint(2, max(3, len(source_table.columns)+1))

    new_schema = generate_schema(new_col_size)
    new_df = generate_table(num_rows=len(key_series.index), column_dict=new_schema, pd=pd, key_series=key_series)
    new_schema[key_col] = source_schema[key_col]

    return new_df, new_schema


def generate_ops_choices(schema: Dict[str, str], num_rows: int, exclude: List[str]=[]) -> Dict[str, Dict]:
    """
    Generate the a number of options for the next operation to be performed on a given table with schema and num_rows
    :param schema: Column Map
    :param num_rows: number of rows in the table
    :return: Dict of ops: args choices
    """
    # Generates parameters for each op as well.

    ops_choices = []
    df_col_types = get_schema_type_mapping(schema)

    logger.debug(f"df_col_types: {df_col_types}")
    '''
    OPS REQUIREMENTS
    assign_numeric = one numeric col, random scalar value
    assign_string = one string col
    groupby = one groupable col, at least one numeric col, random aggregation function
    iloc = two numbers, minimum 10 rows
    sample = random DF fraction
    point_edit = any column. old value, new value
    dropcol = any column
    merge = atleast one joinable column. new dataframe with that joinable column and its values. 
    pivot = one index column, one groupable column and one numeric values column.
    '''

    if 'numeric' in df_col_types:
        # # assign_numeric option
        numeric_col = select_rand_cols(df_col_types, 1, 'numeric')[0]
        # random_scalar = np.random.randint(1, 100, 2)
        # new_col_name = f"{numeric_col}__{str(random_scalar[0])}x + {str(random_scalar[1])}"
        # ops_choices.append((assign_numeric,
        #                     {'numeric_col': numeric_col,
        #                      'random_scalar': random_scalar,
        #                      'new_col_name': new_col_name}
        #                     ))

        if 'groupable' in df_col_types:
            # groupby, pivots now possible
            num_groups = min(np.random.randint(1, 3), len(df_col_types['groupable']))
            group_cols = select_rand_cols(df_col_types, num_groups, 'groupable')
            func = select_rand_aggregate()
            ops_choices.append({'op': 'groupby',
                                'args': {'group_columns': group_cols,
                                         'agg_columns': df_col_types['numeric'],
                                         'agg_function': func}
                                })

            # pivot selections
            if len(df_col_types['groupable']) >= 2:
                index, columns = select_rand_cols(df_col_types, 2, 'groupable')
                values = numeric_col
                ops_choices.append({'op': 'pivot',
                                    'args': {'index_cols': [index], 'columns': [columns], 'value_col': [values],
                                             'agg_func': select_rand_aggregate()}
                                    })

    if 'joinable' in df_col_types:
        on = select_rand_cols(df_col_types, 1, 'joinable')[0]
        ops_choices.append({'op': 'merge', 'args': {'key_col': on}})

    # if 'string' i df_col_types:
    #     #     col = select_rand_cols(df_columns, 1, 'string')[0]
    #     #     new_col_name = col + '__swapcase'
    #     #     ops_choices.append((assign_string, {'col': col, 'new_col_name': new_col_name}))
    #
    #     # Remaining operations are possible if df has at least 10 rows
    #
    if num_rows >= 10:
        frac = get_rand_percentage()
        ops_choices.append({'op': 'sample', 'args': {'frac': frac}})

    if len(schema) > 2:
        num_drop = np.random.randint(1, len(schema)-1, 1)
        ops_choices.append({ 'op': 'project',
                             'args': {
                                'output_cols': np.random.choice(list(schema.keys()), num_drop, replace=False).tolist()
                             }
        })
    #         # # point edits
    #         # col = select_rand_cols(df_columns, 1)[0]
    #         # colvalues = set(df_dict[df_name][col].values)
    #         # old_value = np.random.choice(list(colvalues), 1)[0]
    #         # if col in self.column_mapping:
    #         #     faker_col = self.column_mapping[col]
    #         #     new_value = self.faker.format(faker_col)
    #         #     ops_choices.append((point_edit, {'col': col, 'old_value': old_value, 'new_value': new_value}))
    #
    #     # Dropcol only if dataframe has at least 2 columns
    #     if len(df_columns) >= 2:
    #         col = select_rand_cols(df_columns, 1)[0]
    #         ops_choices.append(('dropcol', {'col': col}))
    #

    # Filter exclusion list of ops here
    ops_choices = list(filter(lambda x: x['op'] not in exclude, ops_choices))

    return ops_choices


def generate_workflow(workflow_class, name='wf', num_versions=10, base_shape=(10, 1000),
                      out_directory='/tmp/dataset', bfactor=1.0, matfreq=1, wf_options={}, exclude_ops=[]):
    """
    Generate a workflow for a given client and parameters
    :param workflow_class: Workflow class to be used (DataFrameWorkflow, ModinWorkflow, or SQLWorkflow)
    :param name: Name for the workflow (Default: 'wf')
    :param num_versions: Number of artifacts to generate (Default 10).
    :param base_shape: tuple of (columns, rows) to generate as the first artifact. Default is (10,1000).
    :param out_directory: output directory to use for generation
    :param bfactor: branch factor for workflow graph (default 1.0)
    :param matfreq: Number of operations to perform before materialization (default 1)
    :param wf_options: Workflow class options as a dict (e.g. SQL string or Modin engine)
    :param exclude_ops: List of string operations to be avoided during generation.
    :return: Workflow object of desired type.
    """
    wf = workflow_class(name=name, out_directory=out_directory, **wf_options)
    wf.generate_base_artifact(num_cols=base_shape[0], num_rows=base_shape[1])

    num_generated = len(wf.artifact_list)
    artifact_exclusions = []
    stop_generation = False

    while num_generated < num_versions:
        try:
            source_artifact = wf.select_random_artifact(bfactor=bfactor, exclude=artifact_exclusions)
            num_ops = 0
            ops_to_do = matfreq  #TODO: Randomize or coin flip here
            force_materialize = False
            logger.info(f"Selected Artifact: {source_artifact}, initializing operation chain")
            wf.initialize_operation(artifacts=[source_artifact])

            # if not source_artifact.schema_map:
            #     break
            # current_schema_map = source_artifact.schema_map

            while num_ops < ops_to_do:
                if num_ops != ops_to_do-1:  # Do not pivot in the middle of an operation chain
                    exclude_ops.append('pivot')

                ops_choices = generate_ops_choices(schema=wf.current_operation.current_schema_map,
                                                   num_rows=len(source_artifact), # TODO: potential num_rows bug
                                                   exclude=exclude_ops)

                if ops_choices:
                    logger.debug(f'Ops Choices: {ops_choices}')
                    selected_op = np.random.choice(ops_choices, 1)[0]
                    source_artifacts = [source_artifact]
                    # TODO: Handle Merge Op here - materialize/execute before adding right artifact
                    if selected_op['op'] == 'merge':
                        if num_generated == num_versions - 1:
                            logger.warning('Attempting to do merge as last operation; doing another op')
                            continue
                        right_df, right_schema = generate_pkfk_join_table(source_table=source_artifact.to_df(),
                                                                          source_schema=source_artifact.schema_map,
                                                                          key_col=selected_op['args']['key_col'])
                        right_df_label = wf.generate_next_label()
                        right_artifact = wf.initialize_new_artifact(label=right_df_label,
                                                                    filename=f"{wf.artifact_dir}/{right_df_label}.csv",
                                                                    schema_map=right_schema)
                        right_artifact.from_df(right_df)
                        wf.add_artifact(right_artifact)
                        wf.current_operation.add_source_artifact(right_artifact)  # TODO: simplify within workflow
                        source_artifacts.append(right_artifact)
                        force_materialize = True

                    try:
                        logger.info(f"Chaining Operation: {selected_op['op']}")
                        wf.chain_to_current_operation([selected_op])
                        if force_materialize:
                            break
                    except NotImplementedError as e:
                        logger.warning(f'Attempting an operation that is not implemented for this workflow type:'
                                       f" {selected_op['op']}")
                        raise e
                    except ValueError as e : # Debugging modin-dask groupby x2 error
                        logger.error(f'Source_artifact {source_artifacts[0].label}, Selected Op: {selected_op}')
                        raise e
                    # TODO: Exception Handling for empty dataframes generated
                    # if not next_df:
                    #    logger.warning('Could not apply_op, retrying...')
                else:
                    logger.warning(f"No ops choices available for {source_artifact.label}")
                    artifact_exclusions.append(source_artifact.label)
                    force_materialize = True
                    if set(artifact_exclusions) == set(wf.artifact_list):
                        logger.warning(f"Do not have any options remaining for any of the artifacts.")
                        stop_generation = True
                    break
                num_ops += 1

            # END while num_ops < ops_to_do - we have chained maximum number of ops
            if num_ops > 0:
                logger.info(f"Executing current operation list: {wf.current_operation}")
                next_label = wf.generate_next_label()
                wf.execute_current_operation(next_label)
            # TODO: exception handling for failed operation chain
            num_generated = len(wf.artifact_list)
            if stop_generation:
                logger.warning(f'Stopping workflow generation early: Completed {num_generated} artifacts')
                break

        except Exception as e:

            logger.error('Error during generation, stopping...')
            logger.error(f'Was trying to execute operation: {wf.current_operation} on soruce artifact(s): {wf.current_operation.sources}')
            logger.error(f'Was trying to execute code: {wf.current_operation.code}')
            logger.error(f'Writing out all files to {wf.out_dir}')
            wf.serialize_workflow()
            raise e

        # TODO Additional Exception Handling Scenarios
        '''
        except pd.errors.EmptyDataError as e:
            print("Empty DF result")
            retries -= 1
            pass
        except ColumnTypeException as e:
            print(e)
            print("Cannot apply operation because of missing column type, skipping")
            retries -= 1
            pass
        except TooSimilarException as e:
            print(e)
            print("Cannot apply operation because generated dataframe is too similar to ones already generated, skipping")
            retries -= 1
            pass
        except Exception as e:
            dataset.lastargs = {}
            print(dataset.lastmatchoice)
            tb = traceback.format_exc()
            errors.append({choice: tb})
            print(dataset.lastargs)
            raise

        if retries == 0:
            dataset.opcount = 0
            dataset.currentdf = None
            chain_retries -= 1
        '''

    wf.serialize_workflow()

    return wf
