import json
import sys
from time import sleep
from pathlib import Path

import requests
from funcx.sdk.client import FuncXClient
from pyhf.contrib.utils import download


def prepare_workspace(data):
    import pyhf

    return pyhf.Workspace(data)


def infer_hypotest(workspace, metadata, doc):
    import time

    import pyhf

    tick = time.time()
    model = workspace.model(
        patches=[doc],
        modifier_settings={
            "normsys": {"interpcode": "code4"},
            "histosys": {"interpcode": "code4p"},
        },
    )
    data = workspace.data(model)
    test_poi = 1.0
    return {
        "metadata": metadata,
        "CLs_obs": float(
            pyhf.infer.hypotest(test_poi, data, model, test_stat="qtilde")
        ),
        "Fit-Time": time.time() - tick,
    }


def count_complete(l):
    return len(list(filter(lambda e: e["result"], l)))


def main():
    NUM_RUNS = 70

    # locally get pyhf pallet for analysis
    if not Path("1Lbb-pallet").exists():
        download("https://doi.org/10.17182/hepdata.90607.v3/r3", "1Lbb-pallet")
    with open("1Lbb-pallet/BkgOnly.json") as bkgonly_json:
        bkgonly_workspace = json.load(bkgonly_json)

    pyhf_endpoint = "a727e996-7836-4bec-9fa2-44ebf7ca5302"

    fxc = FuncXClient()
    fxc.max_requests = 200

    prepare_func = fxc.register_function(prepare_workspace)
    infer_func = fxc.register_function(infer_hypotest)

    prepare_task = fxc.run(
        bkgonly_workspace, endpoint_id=pyhf_endpoint, function_id=prepare_func
    )

    # While this cooks, let's read in the patch set
    patchset = None
    with open("1Lbb-pallet/patchset.json", "r") as readfile:
        patchset = json.load(readfile)
    patch = patchset["patches"][0]
    name = patch["metadata"]["name"]

    w = None

    while not w:
        try:
            w = fxc.get_result(prepare_task)
        except Exception as e:
            print(f"prepare {e}")
            sleep(15)

    print("--------------------")
    print(w)

    NUM_RUNS = len(patchset["patches"])
    tasks = {}
    for i in range(NUM_RUNS):
        patch = patchset["patches"][i]
        name = patch["metadata"]["name"]
        task_id = fxc.run(
            w,
            patch["metadata"],
            patch["patch"],
            endpoint_id=pyhf_endpoint,
            function_id=infer_func,
        )
        tasks[name] = {"id": task_id, "result": None}

    while count_complete(tasks.values()) < NUM_RUNS:
        for task in tasks.keys():
            if not tasks[task]["result"]:
                try:
                    result = fxc.get_result(tasks[task]["id"])
                    print(
                        f"Task {task} complete, there are {count_complete(tasks.values())} results now"
                    )
                    tasks[task]["result"] = result
                except Exception as e:
                    print(e)
                    sleep(15)

    print("--------------------")
    print(tasks.values())


if __name__ == "__main__":
    main()
