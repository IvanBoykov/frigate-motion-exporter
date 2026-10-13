"""Generate vmrule.yaml (VictoriaMetrics VMRule CRD) from alerts-k8s.yml.

alerts-k8s.yml stays the single source of truth: it is what promtool checks
and what alerts-k8s_test.yml exercises. This only repackages it for the
VictoriaMetrics operator, adding per-group params.extra_filters - the filter
vmalert appends to every rule request, which is why no rule carries a job
selector of its own.

Run: python3 prometheus/gen_vmrule.py
"""
import yaml

SRC = "prometheus/alerts-k8s.yml"
OUT = "prometheus/vmrule.yaml"
NAMESPACE = "frigate"
# Matches the recommended evaluation cadence: every `for` in the ruleset is a
# whole number of 1m evaluations. Shorter (e.g. 30s) is harmless - alerts can
# only fire marginally earlier - but multiplies query load for nothing.
GROUP_INTERVAL = "1m"
EXTRA_FILTERS = ['{job="frigate-motion-exporter"}']

HEADER = """# VictoriaMetrics VMRule generated from alerts-k8s.yml by
# prometheus/gen_vmrule.py - edit the source or the generator, not this file.
#
# Each group carries the frigate-motion-exporter job filter as
# params.extra_filters, which vmalert appends to every rule request; rule
# expressions are written without job selectors because of it. The rules
# themselves are identical to the ones promtool test rules validates.
"""


class Dumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        # block sequences indented under their key: k8s convention
        return super().increase_indent(flow, False)


def present_str(dumper, data):
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


Dumper.add_representer(str, present_str)


def main():
    doc = yaml.safe_load(open(SRC))
    groups = [{"name": g["name"],
               "interval": GROUP_INTERVAL,
               "params": {"extra_filters": list(EXTRA_FILTERS)},
               "rules": g["rules"]} for g in doc["groups"]]
    vmrule = {"apiVersion": "operator.victoriametrics.com/v1beta1",
              "kind": "VMRule",
              "metadata": {"name": "frigate-motion-exporter-alerts",
                           "namespace": NAMESPACE},
              "spec": {"groups": groups}}
    body = yaml.dump(vmrule, Dumper=Dumper, sort_keys=False, width=100,
                     allow_unicode=True)
    open(OUT, "w").write(HEADER + body)

    check = yaml.safe_load(open(OUT))
    assert [g["name"] for g in check["spec"]["groups"]] == \
           [g["name"] for g in doc["groups"]]
    for src_g, out_g in zip(doc["groups"], check["spec"]["groups"]):
        assert src_g["rules"] == out_g["rules"], out_g["name"]
        assert out_g["params"] == {"extra_filters": EXTRA_FILTERS}
        assert out_g["interval"] == GROUP_INTERVAL
    n = sum(len(g["rules"]) for g in check["spec"]["groups"])
    print(f"wrote {OUT}: {len(groups)} groups, {n} rules, "
          f"rules identical to {SRC}")


if __name__ == "__main__":
    main()
