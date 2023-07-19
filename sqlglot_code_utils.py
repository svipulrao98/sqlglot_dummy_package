"""
Mock Utils file
"""
import http.server
import re
import json
import webbrowser

import sqlglot
from dbt_artifacts_parser.parser import parse_manifest
from sqlglot.lineage import Node, exp, lineage


def _combine_column_lineage_graphs(col_lineage_1: dict, col_lineage_2: dict):
    """
    Helper function to combine the keys from the 2 given column lineage graphs while preserving the order
    :param col_lineage_1:
    :param col_lineage_2:
    :return:
    """
    result = {}

    # Get the union of keys from both graphs while preserving the order
    keys = list(col_lineage_1.keys()) + [key for key in col_lineage_2.keys() if key not in col_lineage_1]

    for key in keys:
        if key in col_lineage_1 and key in col_lineage_2:
            result[key] = _combine_column_lineage_graphs(col_lineage_1[key], col_lineage_2[key])
        elif key in col_lineage_1:
            result[key] = col_lineage_1[key]
        else:
            result[key] = col_lineage_2[key]

    return result


def _add_source_tables_to_json(lineage_graph: dict) -> dict:
    """
    Add the leaf/source tables to the JSON
    :param lineage_graph:
    :return:
    """
    _ret_lineage_graph = lineage_graph.copy()
    for _model in lineage_graph.keys():
        for _model_column in lineage_graph[_model].keys():
            for _table, _cols in lineage_graph[_model][_model_column].items():
                if _table not in _ret_lineage_graph.keys():
                    _ret_lineage_graph[_table] = _cols
                elif isinstance(_ret_lineage_graph[_table], list):
                    _ret_lineage_graph[_table].extend(_cols)

    return _ret_lineage_graph



def _fetch_column_names_for_query(_query: str) -> list:
    """
    Fetch all the column names(alias) for a given query
    :param _query:
    :return:
    """
    column_names = []
    for expression in sqlglot.parse_one(_query).find(exp.Select).args["expressions"]:
        if isinstance(expression, exp.Alias):
            column_names.append(expression.text("alias"))
        elif isinstance(expression, exp.Column):
            column_names.append(expression.text("this"))
    return column_names


def remove_comments(str1: str = "") -> str:
    """
    Remove comments/excessive spaces/"create table as"/"create view as" from the sql file
    :param str1: the original sql
    :return: the parsed sql
    """
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", str1)
    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])
    # replace all spaces around commas
    q = re.sub(r"\s*,\s*", ",", q)
    # replace all multiple spaces to one space
    str1 = re.sub("\s\s+", " ", q)
    str1 = str1.replace("\n", " ").strip()
    return str1


def _preprocess_sql(node: "ModelNode" = None) -> str:
    """
    Process the sql, remove database name in the clause/datetime_add/datetime_sub adding quotes
    :param node: the node containing the original sql, file: file name for the sql
    :return: None
    """
    if node is None:
        return ""
    org_sql = node.compiled_code
    ret_sql = remove_comments(str1=org_sql)
    ret_sql = ret_sql.replace("`", "\"")
    # remove any database names in the query
    schema = node.schema_
    #     for i in schemas:
    #         ret_sql = re.sub("[^ (,]*(\.{}\.)".format(i), "{}.".format(i), ret_sql)
    ret_sql = re.sub("[^ (,]*(\.{}\.)".format(schema), "{}.".format(schema), ret_sql)
    ret_sql = re.sub(
        r"DATETIME_DIFF\((.+?),\s?(.+?),\s?(DAY|MINUTE|SECOND|HOUR|YEAR)\)",
        r"DATETIME_DIFF(\1, \2, '\3'::TEXT)",
        ret_sql,
    )
    ret_sql = re.sub("datetime_add", "DATETIME_ADD", ret_sql, flags=re.IGNORECASE)
    ret_sql = re.sub("datetime_sub", "DATETIME_SUB", ret_sql, flags=re.IGNORECASE)
    # DATETIME_ADD '' value
    dateime_groups = re.findall(
        r"DATETIME_ADD\(\s?(.+?),\s?INTERVAL\s?(.+?)\s?(DAY|MINUTE|SECOND|HOUR|YEAR)\)",
        ret_sql,
    )
    if dateime_groups:
        for i in dateime_groups:
            if not i[1].startswith("'") and not i[1].endswith("'"):
                ret_sql = ret_sql.replace(
                    "DATETIME_ADD({},INTERVAL {} {})".format(i[0], i[1], i[2]),
                    "DATETIME_ADD({},INTERVAL '{}' {})".format(i[0], i[1], i[2]),
                )
            else:
                continue
    # DATETIME_SUB '' value
    dateime_sub_groups = re.findall(
        r"DATETIME_SUB\(\s?(.+?),\s?INTERVAL\s?(.+?)\s?(DAY|MINUTE|SECOND|HOUR|YEAR)\)",
        ret_sql,
    )
    if dateime_sub_groups:
        for i in dateime_sub_groups:
            if not i[1].startswith("'") and not i[1].endswith("'"):
                ret_sql = ret_sql.replace(
                    "DATETIME_SUB({},INTERVAL {} {})".format(i[0], i[1], i[2]),
                    "DATETIME_SUB({},INTERVAL '{}' {})".format(i[0], i[1], i[2]),
                )
            else:
                continue
    return ret_sql


def form_sources(manifest_file_name: str):
    """
    Form the sources
    :param manifest_file_name:
    :return:
    """
    with open(manifest_file_name, "r") as fp:
        manifest_dict = json.load(fp)
        manifest_obj = parse_manifest(manifest=manifest_dict)

    sources = {}
    for node_name, node_obj in manifest_obj.nodes.items():
        if "ModelNode" in node_obj.__class__.__name__:
            sources[node_name] = _preprocess_sql(node_obj)

    source_tables = [_source_table.split('.')[-1] for _source_table in manifest_obj.sources.keys()]

    return sources, source_tables, manifest_obj


def get_lineage(nodes: Node, dialect: str = "postgres") -> (list, dict):
    """
    Get Lineage from the Nodes
    :param nodes:
    :param dialect:
    :return:
    """
    nodes_ = {}
    edges = []
    for node in nodes.walk():
        if isinstance(node.expression, exp.Table):
            label = f"FROM {node.expression.this}"
            title = f"<pre>SELECT {node.name} FROM {node.expression.this}</pre>"
            group = 1
        else:
            label = node.expression.sql(pretty=True, dialect='postgres')
            source = node.source.transform(
                lambda n: exp.Tag(this=n, prefix="<b>", postfix="</b>")
                if n is node.expression
                else n,
                copy=False,
            ).sql(pretty=True, dialect=dialect)
            title = f"<pre>{source}</pre>"
            group = 0

        node_id = id(node)

        nodes_[node_id] = {
            "id": node_id,
            "label": label,
            "title": title,
            "group": group,
        }

        for d in node.downstream:
            edges.append({"from": node_id, "to": id(d)})
    return edges, nodes_


def get_complete_column_lineage(column_name: str, final_model: str, sources: dict, dialect: str = 'postgres'):
    """
    Get the complete column lineage
    :param column_name:
    :param final_model:
    :param sources:
    :param dialect:
    :return:
    """
    current_model = final_model
    current_column = column_name
    final_edges = []
    final_nodes = {}
    while True:
        leaf_reached = True
        node = lineage(
            current_column,
            sources.get(current_model),
            sources=sources,
            dialect='postgres'
        )
        edges, nodes = get_lineage(node, dialect)
        final_edges.extend(edges)
        final_nodes.update(nodes)
        current_column = nodes[edges[0]["from"]]["label"]
        if "AS" in current_column:
            current_column = current_column.split(" AS ")[-1]
        current_model = nodes[edges[-1]["to"]]["label"].split(" ")[-1]
        print(current_column)
        print(current_model)
        for _models in sources.keys():
            if current_model == _models.split('.')[-1]:
                leaf_reached = False
                current_model = _models
                break
        if leaf_reached:
            return final_edges, final_nodes


def get_complete_column_lineage_2(column_name: str, final_model: str, sources: dict, source_tables: list):
    """
    Get the complete column lineage
    :param column_name:
    :param final_model:
    :param sources:
    :param source_tables:
    :return:
    """
    current_models = [final_model]
    current_columns = [column_name]
    final_graph = {}
    while True:
        node = lineage(
            current_columns[0],
            sources.get(current_models[0]),
            sources=sources,
            dialect='postgres'
        )

        _verify_query_is_not_subquery = node.downstream[0].name.split('.')[-2]
        for _model in sources.keys():
            if _verify_query_is_not_subquery == _model.split('.')[-1]:
                _verify_query_is_not_subquery = False
                break
        if _verify_query_is_not_subquery in source_tables:
            _verify_query_is_not_subquery = False
        if _verify_query_is_not_subquery:
            node = node.downstream[0]
        _col_name = current_columns.pop(0)
        _table_name = current_models.pop(0).split('.')[-1]
        _curr_downstream = {}
        for _downstream in node.downstream:
            _downstream_table_name = _downstream.name.split('.')[-2]
            _downstream_col_name = _downstream.name.split('.')[-1]
            for _model in sources.keys():
                if _downstream_table_name == _model.split('.')[-1]:
                    current_columns.append(_downstream_col_name)
                    current_models.append(_model)
            if _downstream_table_name in _curr_downstream.keys():
                _curr_downstream[_downstream_table_name].append(_downstream_col_name)
            else:
                _curr_downstream[_downstream_table_name] = [_downstream_col_name]
        if _table_name in final_graph.keys():
            if _col_name in final_graph[_table_name].keys():
                for _downstream_table_name in _curr_downstream.keys():
                    if _downstream_table_name in final_graph[_table_name][_col_name].keys():
                        final_graph[_table_name][_col_name][_downstream_table_name].\
                            extend(_curr_downstream[_downstream_table_name])
                    else:
                        final_graph[_table_name][_col_name][_downstream_table_name] = \
                            _curr_downstream[_downstream_table_name]
            else:
                final_graph[_table_name][_col_name] = _curr_downstream
        else:
            final_graph[_table_name] = {
                _col_name: _curr_downstream
            }
        if not current_columns:
            return final_graph


def get_lineage_for_all_columns(column_names: list, final_model: str, sources: dict, source_tables: list) -> dict:
    """
    Get lineage for all the columns
    :param column_names:
    :param final_model:
    :param sources:
    :param source_tables:
    :return:
    """
    lineage_for_all_columns = {}
    for column in column_names:
        lineage_for_all_columns = _combine_column_lineage_graphs(
            lineage_for_all_columns,
            get_complete_column_lineage_2(column, final_model, sources, source_tables)
        )

    return _add_source_tables_to_json(lineage_for_all_columns)


def serve_lineage_html(html_lineage_file: str = "http://localhost:8000/dbt_packages/sqlglot_dummy_package/tp.html",
                       port: int = 8000):
    """
    Serve HTML file
    :param html_lineage_file:
    :param port:
    :return:
    """
    server_address = ("", port)
    handler = http.server.SimpleHTTPRequestHandler
    httpd = http.server.HTTPServer(server_address, handler)
    webbrowser.open_new_tab(html_lineage_file)
    httpd.serve_forever()

# queries = []
# with open(
#     "/Users/anandgupta/codebase/altimate-scripts/sql_profiler/src/dbt/sample_data/manifest.json",
#     "r",
# ) as fp:
#     manifest_dict = json.load(fp)
#     manifest_obj = parse_manifest(manifest=manifest_dict)
#     print(manifest_obj.nodes)
#     for node_name, node_obj in manifest_obj.nodes.items():
#         if "ModelNode" in node_obj.__class__.__name__:
#             print(node_obj.resource_type)
#             table_name = table_name = node_obj.schema_ + "." + node_obj.name
#             print(node_obj.original_file_path)
#             print(node_obj.compiled_code)
#             cleaned_query = remove_comments(node_obj.compiled_code)
#             queries.append(cleaned_query)
#             print(node_obj.description)
#             print(node_obj.columns)
#             print(node_obj.tags)
#             print(node_obj.config)
# print("*" * 100)
# print(queries)
# print("*" * 100)
