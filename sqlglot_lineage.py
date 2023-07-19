import json
import os
import pickle
import webbrowser

from sqlglot.lineage import lineage

from jaffle_shop.dbt_packages.sqlglot_dummy_package.sqlglot_code_utils import form_sources, \
    get_lineage_for_all_columns, serve_lineage_html

if __name__ == '__main__':

    sources, source_tables, manifest_parsed_obj = form_sources(
        "/Users/vips/Altimate - AI/dbt-tutorial/jaffle_shop/target/manifest.json")
    model_name = 'model.vips_dbt_test.combined_model'
    column_name = "policy_type"
    column_names = [column_name, "alias"]
    # column_names = list(manifest_parsed_obj.nodes[model_name].columns.keys())

    lineage_graph = get_lineage_for_all_columns(
        column_names,
        model_name,
        sources=sources,
        source_tables=source_tables
    )
    print(os.getcwd())
    with open(f"{os.getcwd()}/dbt_packages/sqlglot_dummy_package/lineage_op.json", "w") as _lin:
        json.dump(lineage_graph, _lin, indent=3)

    serve_lineage_html()

    # WORK FOR AUTOMATED SERVING ON THE DBT DOCS PAGE ITSELF
    func = '(t.openColumnLineage = function () { var modal = document.getElementById("myModal"); var modalIframe = document.getElementById("modalIframe"); var closeButton = document.getElementsByClassName("close")[0]; modalIframe.src = "/dbt-tutorial/jaffle_shop/dbt_packages/sqlglot_dummy_package/tp.html"; modalIframe.style.height = window.innerHeight * 0.9 + "px"; modalIframe.style.width = window.innerWidth * 0.8 + "px"; myModal.style.padding = window.innerHeight * 0.02 + "px"; modal.style.display = "block"; document.body.style.overflow = "hidden"; closeButton.addEventListener("click", function() { modal.style.display = "none"; document.body.style.overflow = ""; }); window.addEventListener("click", function(event) { if (event.target === modal) { modal.style.display = "none"; document.body.style.overflow = ""; } }); }),'

    html = '<div class="launcher-btn">\n    <a class="btn btn-info btn-pill btn-lg btn-icon btn-shadow" data-toggle="tooltip"\n        title="View Column Lineage" ng-click="openColumnLineage()">\n        <svg class="icn icn-md">\n            <use xlink:href="#icn-flow"></use>\n        </svg>\n    </a>\n</div>\n<div id="myModal" class="modal">\n    <div class="modal-content">\n        <span class="close">&times;</span>\n        <iframe id="modalIframe"></iframe>\n    </div>\n</div>\n<br />'

    css = '.modal {\n    display: none;\n    position: fixed;\n    z-index: 1;\n    left: 0;\n    top: 0;\n    width: 100%;\n    height: 100%;\n    overflow: auto;\n    background-color: rgba(0, 0, 0, 0.5);\n}\n\n.modal-content {\n    background-color: #fefefe;\n    margin: 5% auto;\n    padding: 20px;\n    border: 1px solid #888;\n    width: 90%;\n    height: 90%;\n    position: relative;\n}\n\n.close {\n    color: #aaa;\n    position: absolute;\n    top: 10px;\n    right: 25px;\n    font-size: 28px;\n    font-weight: bold;\n    cursor: pointer;\n}\n\n.close:hover,\n.close:focus {\n    color: black;\n    text-decoration: none;\n    cursor: pointer;\n}\n\niframe {\n    width: 100%;\n    height: 100%;\n    border: none;\n}'
