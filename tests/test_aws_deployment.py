# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Exercise deployment selection and storage setup without cloud access."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
AWS = ROOT / "deploy" / "aws"


@pytest.fixture
def aws_environment(tmp_path: Path) -> dict[str, str]:
    for tool in ("bash", "jq", "yq", "envsubst"):
        if not shutil.which(tool):
            pytest.skip(f"requires {tool}")
    binary = tmp_path / "bin"
    binary.mkdir()
    stub = binary / "stub"
    stub.write_text(
        f"#!{sys.executable}\n"
        + """import json, os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["CALLS"], "a") as stream:
    stream.write(json.dumps([name, *args]) + "\\n")
if name == "aws":
    service, action = args[:2]
    if os.environ.get("TEST_AWS_WARNING") == action:
        print("warning: test diagnostic", file=sys.stderr)
    if service == "iam" and os.environ.get("TEST_IAM_ERROR_ACTION") == action:
        code = os.environ.get("TEST_IAM_ERROR_CODE", "AccessDenied")
        sys.exit(f"An error occurred ({code}) when calling {action}")
    msk_ec2 = {"describe-subnets", "describe-security-groups", "create-security-group",
               "authorize-security-group-ingress", "describe-vpcs", "delete-security-group"}
    if os.environ.get("TEST_NO_MSK") == "true" and (service == "kafka" or (service == "ec2" and action in msk_ec2)):
        sys.exit("MSK API called when disabled")
    if (service, action) == ("sts", "get-caller-identity"):
        print("123456789012")
    elif (service, action) == ("eks", "describe-cluster"):
        if os.environ.get("TEST_CLUSTER_ERROR"):
            sys.exit("An error occurred (AccessDeniedException) when calling the DescribeCluster operation")
        if os.environ.get("TEST_EKS_ABSENT"):
            sys.exit("An error occurred (ResourceNotFoundException) when calling the DescribeCluster operation")
        print(json.dumps({"cluster": {"resourcesVpcConfig": {"vpcId": "vpc-1", "subnetIds": ["s1", "s2"]}}}))
    elif (service, action) == ("iam", "get-role") and os.environ.get("TEST_ROLE_ABSENT"):
        sys.exit("An error occurred (NoSuchEntity) when calling the GetRole operation")
    elif (service, action) == ("iam", "get-role"):
        tags = [] if os.environ.get("TEST_ROLE_UNTAGGED") else [{"Key": "lakehouse-ingest-bench", "Value": "true"}]
        arn = "arn:aws:iam::123456789012:role/lakehouse-ingest-bench-harness"
        print(json.dumps({"Role": {"Arn": arn, "Tags": tags}}))
    elif (service, action) in {("ec2", "describe-regions"), ("eks", "list-clusters"),
                              ("eks", "list-pod-identity-associations"),
                              ("eks", "describe-pod-identity-association")}:
        if os.environ.get("TEST_IDENTITY_ERROR") == action:
            sys.exit("identity lookup denied")
        if action == "describe-regions":
            print("us-west-2\\tus-east-2")
        elif action == "list-clusters":
            print("peer" if args[args.index("--region") + 1] == "us-east-2" else "bench")
        elif action == "list-pod-identity-associations":
            cluster = args[args.index("--cluster-name") + 1]
            if "--namespace" not in args and os.environ.get("TEST_ROLE_SHARED") == cluster:
                print("assoc-peer")
        else:
            role = "arn:aws:iam::123456789012:role/lakehouse-ingest-bench-harness"
            print("arn:aws:iam::123456789012:role/peer\\t" + role if os.environ.get("TEST_TARGET_ROLE_ONLY") else role)
    elif (service, action) == ("s3api", "get-bucket-tagging"):
        print("true")
    elif (service, action) == ("s3api", "head-bucket") and os.environ.get("TEST_BUCKET_ABSENT"):
        sys.exit("An error occurred (404) when calling the HeadBucket operation: Not Found")
    elif (service, action) == ("s3api", "get-bucket-versioning"):
        print(os.environ.get("TEST_BUCKET_VERSIONING", "None"))
    elif (service, action) == ("iam", "put-role-policy"):
        pathlib.Path(os.environ["POLICY"]).write_text(args[args.index("--policy-document") + 1])
    elif (service, action) == ("ec2", "describe-subnets"):
        print(json.dumps({"Subnets": [
            {"SubnetId": "s1", "AvailabilityZone": "a", "MapPublicIpOnLaunch": False},
            {"SubnetId": "s2", "AvailabilityZone": "b", "MapPublicIpOnLaunch": False}]}))
    elif (service, action) == ("ec2", "describe-security-groups") and "json" in args:
        tags = [] if os.environ.get("TEST_SG_UNTAGGED") else [{"Key": "lakehouse-ingest-bench", "Value": "true"}]
        print(json.dumps({"GroupId": "sg-1", "Tags": tags}))
    elif (service, action) == ("ec2", "describe-security-groups"):
        print("sg-1")
    elif (service, action) == ("kafka", "list-tags-for-resource"):
        print("None" if os.environ.get("TEST_MSK_UNTAGGED") else "true")
    elif (service, action) == ("ecr", "describe-repositories"):
        if os.environ.get("TEST_ECR_ERROR"):
            sys.exit("An error occurred (" + os.environ["TEST_ECR_ERROR"] + ") when calling DescribeRepositories")
        print("arn:aws:ecr:us-west-2:123456789012:repository/" + args[args.index("--repository-names") + 1])
    elif (service, action) == ("ecr", "list-tags-for-resource"):
        print("None" if os.environ.get("TEST_ECR_UNTAGGED") else "true")
    elif (service, action) == ("ec2", "describe-vpcs"):
        print("10.0.0.0/16")
    elif (service, action) == ("ec2", "describe-route-tables"):
        print(os.environ.get("TEST_ROUTE_TABLES", "rtb-1\\trtb-2"))
    elif (service, action) == ("ec2", "describe-vpc-endpoints"):
        endpoint = os.environ.get("TEST_ENDPOINT", "")
        owned = [{"Key": "lakehouse-ingest-bench", "Value": "true"}]
        endpoints = {
            "": [],
            "partial": [{"VpcEndpointId": "vpce-1", "RouteTableIds": ["rtb-1"], "Tags": owned}],
            "partial-foreign": [{"VpcEndpointId": "vpce-9", "RouteTableIds": ["rtb-1"]}],
            "complete": [{"VpcEndpointId": "vpce-1", "RouteTableIds": ["rtb-1", "rtb-2"]}],
            "split": [
                {"VpcEndpointId": "vpce-1", "RouteTableIds": ["rtb-1"], "Tags": owned},
                {"VpcEndpointId": "vpce-2", "RouteTableIds": ["rtb-2"]},
            ],
        }[endpoint]
        if any(arg.startswith("Name=tag:") for arg in args):
            print("vpce-1" if endpoint else "None")
        else:
            print(json.dumps(endpoints))
    elif (service, action) == ("ec2", "delete-vpc-endpoints"):
        failures = []
        if os.environ.get("TEST_ENDPOINT_DELETE_ERROR"):
            failures.append({"ResourceId": "vpce-1", "Error": {
                "Code": "UnauthorizedOperation", "Message": "endpoint deletion denied"}})
        print(json.dumps({"Unsuccessful": failures}))
    elif (service, action) == ("kafka", "list-clusters"):
        print("None" if os.environ.get("TEST_MSK_ABSENT") else "arn:aws:kafka:us-west-2:123456789012:cluster/bench/id")
    elif (service, action) == ("kafka", "describe-cluster"):
        if os.environ.get("TEST_MSK_DELETE_FINISH") and "--query" not in args:
            sys.exit("An error occurred (NotFoundException) when calling DescribeCluster")
        query = args[args.index("--query") + 1]
        print("ACTIVE\\tv1\\t100" if "VolumeSize" in query else "ACTIVE")
    elif (service, action) == ("kafka", "get-bootstrap-brokers"):
        print("broker.example:9098")
elif name == "kubectl":
    if "crd" in args and os.environ.get("TEST_CRD_ABSENT"):
        sys.exit("Error from server (NotFound): customresourcedefinitions not found")
    if "get-contexts" in args:
        print("bench")
    elif "get" in args and "namespace" in args:
        if os.environ.get("TEST_NAMESPACE_WARNING"):
            print("Warning: test credential warning", file=sys.stderr)
        if os.environ.get("TEST_NAMESPACE_ERROR"):
            sys.exit("Error from server (Forbidden): namespaces is forbidden")
        if not os.environ.get("TEST_NAMESPACE_ABSENT"):
            print("namespace/ingest-bench")
    elif "delete" in args and "namespace" in args and os.environ.get("TEST_NAMESPACE_ABSENT"):
        sys.exit("Error from server (NotFound): namespace not found")
    elif "nodes" in args:
        zones = json.loads(os.environ.get("TEST_NODE_ZONES", '["us-west-2a"]'))
        print(json.dumps({"items": [
            {"metadata": {"labels": {"topology.kubernetes.io/zone": zone} if zone else {}},
             "status": {"nodeInfo": {"architecture": "amd64"}}} for zone in zones]}))
    elif "apply" in args:
        sys.stdin.read()
elif name == "helm" and "status" in args:
    print(json.dumps({"info": {"status": "deployed"}}))
elif name == "helm" and "values" in args and "strimzi-kafka-operator" in args:
    print('{}')
elif name == "helm":
    print('[{"chart": "operator-1.0"}]')
elif name == "helmfile":
    assert os.environ["CATALOG_STORAGE_CLASS"] == "ingest-bench-catalog"
"""
    )
    stub.chmod(0o755)
    for name in ("aws", "kubectl", "helm", "helmfile"):
        (binary / name).symlink_to(stub)
    environment = dict(os.environ)
    for key in ("MSK_BROKERS", "MSK_VOLUME_GIB", "WITH_MSK"):
        environment.pop(key, None)
    environment.update(
        PATH=f"{binary}:{os.environ['PATH']}",
        AWS_REGION="us-west-2",
        CLUSTER_NAME="bench",
        KUBE_CONTEXT="bench",
        NAMESPACE="ingest-bench",
        BUCKET="bench-bucket",
        WITH_SCHEMA_REGISTRY="false",
        CALLS=str(tmp_path / "calls.jsonl"),
        POLICY=str(tmp_path / "policy.json"),
    )
    site = tmp_path / "input-site.yaml"
    site.write_text("kafka:\n  deployment: in-cluster\n")
    environment["SITE_FILE"] = str(site)
    environment["CLOUD"] = "aws"
    return environment


def _calls(environment: dict[str, str]) -> list[list[str]]:
    return [json.loads(line) for line in Path(environment["CALLS"]).read_text().splitlines()]


def _is_endpoint_call(call: list[str]) -> bool:
    return call[:2] == ["aws", "ec2"] and call[2] in {
        "describe-route-tables",
        "describe-vpc-endpoints",
        "create-vpc-endpoint",
        "modify-vpc-endpoint",
        "delete-vpc-endpoints",
    }


@pytest.mark.parametrize(
    "existing,expected",
    [
        ("", "create-vpc-endpoint"),
        ("partial", "modify-vpc-endpoint"),
        ("partial-foreign", "create-vpc-endpoint"),
        ("complete", None),
    ],
)
def test_setup_routes_s3_through_a_gateway_endpoint(
    aws_environment: dict[str, str], existing: str, expected: str | None
) -> None:
    """Create or extend S3 gateway endpoint routing without changing complete coverage."""
    aws_environment["TEST_ENDPOINT"] = existing
    aws_environment["TEST_NO_MSK"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh"), "--site", aws_environment["SITE_FILE"]],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    changes = [call for call in _calls(aws_environment) if call[2] in ("create-vpc-endpoint", "modify-vpc-endpoint")]
    if expected is None:
        assert not changes
        assert "covers every route table" in result.stderr
        return
    assert len(changes) == 1 and changes[0][2] == expected
    change = changes[0]
    if expected == "create-vpc-endpoint":
        assert change[change.index("--vpc-id") + 1] == "vpc-1"
        assert change[change.index("--vpc-endpoint-type") + 1] == "Gateway"
        assert change[change.index("--service-name") + 1] == "com.amazonaws.us-west-2.s3"
        tables = change.index("--route-table-ids")
        expected_tables = ["rtb-2"] if existing == "partial-foreign" else ["rtb-1", "rtb-2"]
        assert change[tables + 1 : tables + 1 + len(expected_tables)] == expected_tables
        assert not change[tables + 1 + len(expected_tables)].startswith("rtb-")
        assert "Key=lakehouse-ingest-bench,Value=true" in change[change.index("--tag-specifications") + 1]
    else:
        assert change[change.index("--vpc-endpoint-id") + 1] == "vpce-1"
        assert change[change.index("--add-route-table-ids") + 1 :] == ["rtb-2"]


@pytest.mark.parametrize("existing", ["", "complete"])
def test_teardown_all_removes_only_the_tagged_s3_endpoint(aws_environment: dict[str, str], existing: str) -> None:
    aws_environment["TEST_ENDPOINT"] = existing
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh"), "--all", "--yes"],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    calls = _calls(aws_environment)
    lookups = [call for call in calls if call[2:3] == ["describe-vpc-endpoints"]]
    deletes = [call for call in calls if call[2:3] == ["delete-vpc-endpoints"]]
    assert len(lookups) == 1
    assert "Name=vpc-id,Values=vpc-1" in lookups[0] and "Name=tag:lakehouse-ingest-bench,Values=true" in lookups[0]
    if existing:
        assert len(deletes) == 1 and deletes[0][deletes[0].index("--vpc-endpoint-ids") + 1] == "vpce-1"
    else:
        assert not deletes


@pytest.mark.parametrize("uncovered", [False, True])
def test_setup_preserves_routes_covered_by_multiple_s3_endpoints(
    aws_environment: dict[str, str],
    uncovered: bool,
) -> None:
    aws_environment["TEST_ENDPOINT"] = "split"
    if uncovered:
        aws_environment["TEST_ROUTE_TABLES"] = "rtb-1\trtb-2\trtb-3"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh")],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    changes = [
        call for call in _calls(aws_environment) if call[2:3] in (["create-vpc-endpoint"], ["modify-vpc-endpoint"])
    ]
    if uncovered:
        assert changes == [
            [
                "aws",
                "ec2",
                "modify-vpc-endpoint",
                "--vpc-endpoint-id",
                "vpce-1",
                "--add-route-table-ids",
                "rtb-3",
            ]
        ]
    else:
        assert not changes


def test_teardown_stops_when_endpoint_deletion_reports_failure(aws_environment: dict[str, str]) -> None:
    aws_environment["TEST_ENDPOINT"] = "complete"
    aws_environment["TEST_ENDPOINT_DELETE_ERROR"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh"), "--all", "--yes"],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "vpce-1" in result.stderr
    assert "endpoint deletion denied" in result.stderr
    assert not any(call[2:3] == ["delete-bucket"] for call in _calls(aws_environment))


@pytest.mark.parametrize(
    "zones,logged,warning",
    [
        ([], "unknown", False),
        ([None], "unknown", False),
        (["us-west-2a", "us-west-2a", None], "us-west-2a", False),
        (["us-west-2b", "us-west-2a", "us-west-2b"], "us-west-2a us-west-2b", True),
    ],
)
def test_setup_warns_only_when_nodes_span_multiple_zones(
    aws_environment: dict[str, str],
    zones: list[str | None],
    logged: str,
    warning: bool,
) -> None:
    aws_environment["TEST_NODE_ZONES"] = json.dumps(zones)
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh")],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"node availability zones: {logged}" in result.stderr
    assert ("benchmark traffic may incur cross-AZ charges" in result.stderr) == warning
    assert sum("nodes" in call and call[0] == "kubectl" for call in _calls(aws_environment)) == 1


@pytest.mark.parametrize("deployment", ["managed", "in-cluster", "external"])
def test_setup_follows_configured_deployment(aws_environment: dict[str, str], tmp_path: Path, deployment: str) -> None:
    source = {
        "kafka": {
            "deployment": deployment,
            "bootstrap_servers": "external.example:9093",
            "security": {"security.protocol": "SASL_SSL", "sasl.password": "${env:PASSWORD}"},
            "schema_registry": {"url": "https://registry.example"},
        },
        "catalog": {"props": {"uri": "https://catalog.example", "warehouse": "existing"}},
    }
    Path(aws_environment["SITE_FILE"]).write_text(yaml.safe_dump(source))
    # A stale opt-out flag cannot override the site in either direction.
    aws_environment["WITH_MSK"] = "false" if deployment == "managed" else "true"
    if deployment != "managed":
        aws_environment["TEST_NO_MSK"] = "true"
        aws_environment["MSK_BROKERS"] = "unused"
        aws_environment["MSK_VOLUME_GIB"] = "unused"
        aws_environment["WITH_SCHEMA_REGISTRY"] = "true"
    site = tmp_path / "site.yaml"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh"), "--site", aws_environment["SITE_FILE"], "--write-site", str(site)],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    policy = json.loads(Path(aws_environment["POLICY"]).read_text())
    settings = yaml.safe_load(site.read_text())
    assert settings["kafka"]["deployment"] == deployment
    statements = {statement["Sid"] for statement in policy["Statement"]}
    assert "ReadAndWriteTheThreePrefixes" in statements
    assert ("GlueIcebergCatalog" in statements) == (deployment != "in-cluster")
    if deployment != "managed":
        assert not any(sid.startswith("Msk") for sid in statements)
        calls = _calls(aws_environment)
        # The S3 endpoint is reconciled for every deployment; MSK's EC2 calls are not.
        assert not any(call[1] == "kafka" or (call[1] == "ec2" and not _is_endpoint_call(call)) for call in calls)
        assert "waiting for lakehouse-ingest-bench to reach ACTIVE" not in result.stdout
    if deployment == "in-cluster":
        assert settings["kafka"]["security"] == {}
        assert settings["kafka"]["bootstrap_servers"] == "ingest-bench-kafka-bootstrap.ingest-bench.svc:9092"
        assert settings["kafka"]["schema_registry"]["url"] == (
            "http://schema-registry.ingest-bench.svc:8080/apis/ccompat/v7"
        )
        assert settings["catalog"]["props"] == {
            "uri": "http://lakekeeper.ingest-bench.svc:8181/catalog",
            "warehouse": "ingest-bench",
            "s3.region": "us-west-2",
        }
        assert "glue." not in result.stdout
    elif deployment == "external":
        assert settings["kafka"] == source["kafka"]
        assert settings["catalog"] == source["catalog"]
    else:
        assert {"MskCluster", "MskTopics", "MskGroups"} <= statements
        assert settings["kafka"]["security"]["sasl.mechanism"] == "OAUTHBEARER"
        assert settings["kafka"]["bootstrap_servers"] == "broker.example:9098"


@pytest.mark.parametrize("deployment", ["in-cluster", "external"])
def test_teardown_skips_msk_for_other_deployments(aws_environment: dict[str, str], deployment: str) -> None:
    Path(aws_environment["SITE_FILE"]).write_text(f"kafka:\n  deployment: {deployment}\n")
    aws_environment["WITH_MSK"] = "true"
    aws_environment["TEST_NO_MSK"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
    calls = _calls(aws_environment)
    assert not any(call[:2] == ["aws", "kafka"] for call in calls)
    assert all(call[2] == "describe-regions" for call in calls if call[:2] == ["aws", "ec2"])
    assert any(call[:3] == ["aws", "iam", "delete-role"] for call in calls)


@pytest.mark.parametrize("failure", ["TEST_CLUSTER_ERROR", "TEST_NAMESPACE_ERROR"])
def test_teardown_stops_before_identity_deletion_on_lookup_failure(
    aws_environment: dict[str, str], failure: str
) -> None:
    aws_environment[failure] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert not any(
        call[:3]
        in (
            ["aws", "iam", "delete-role"],
            ["aws", "iam", "delete-role-policy"],
            ["aws", "eks", "delete-pod-identity-association"],
        )
        for call in _calls(aws_environment)
    )


@pytest.mark.parametrize("absent", ["TEST_EKS_ABSENT", "TEST_NAMESPACE_ABSENT"])
def test_teardown_allows_confirmed_absence(aws_environment: dict[str, str], absent: str) -> None:
    aws_environment[absent] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr
    assert any(call[:3] == ["aws", "iam", "delete-role"] for call in _calls(aws_environment))


@pytest.mark.parametrize(
    "action",
    [
        "describe-cluster",
        "get-role",
        "describe-regions",
        "list-clusters",
        "list-pod-identity-associations",
        "describe-pod-identity-association",
    ],
)
def test_teardown_keeps_warnings_separate_from_lookup_data(aws_environment: dict[str, str], action: str) -> None:
    aws_environment["TEST_AWS_WARNING"] = action
    aws_environment["TEST_ROLE_SHARED"] = "peer"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr
    assert "warning: test diagnostic" in result.stderr
    calls = _calls(aws_environment)
    assert not any("warning:" in call for call in calls)
    assert not any(call[:3] in (["aws", "iam", "delete-role"], ["aws", "iam", "delete-role-policy"]) for call in calls)


@pytest.mark.parametrize("absent", [False, True])
def test_teardown_namespace_warnings_do_not_change_presence(aws_environment: dict[str, str], absent: bool) -> None:
    aws_environment["TEST_NAMESPACE_WARNING"] = "true"
    if absent:
        aws_environment["TEST_NAMESPACE_ABSENT"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr
    assert "Warning: test credential warning" in result.stderr
    calls = _calls(aws_environment)
    namespace_deleted = any(call[0] == "kubectl" and "delete" in call and "namespace" in call for call in calls)
    assert namespace_deleted == (not absent)
    assert any(call[:3] == ["aws", "iam", "delete-role"] for call in calls)


@pytest.mark.parametrize("action", ["get-role", "get-role-policy", "list-clusters"])
def test_teardown_removes_msk_before_shared_role_lookup_failure(aws_environment: dict[str, str], action: str) -> None:
    Path(aws_environment["SITE_FILE"]).write_text("kafka: {deployment: managed}\n")
    aws_environment["TEST_MSK_DELETE_FINISH"] = "true"
    aws_environment["TEST_IDENTITY_ERROR" if action == "list-clusters" else "TEST_IAM_ERROR_ACTION"] = action
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    calls = _calls(aws_environment)
    broker_delete = next(i for i, call in enumerate(calls) if call[:3] == ["aws", "kafka", "delete-cluster"])
    role_lookup = next(i for i, call in enumerate(calls) if call[:3] == ["aws", "iam", "get-role"])
    assert broker_delete < role_lookup
    assert any(call[:3] == ["aws", "ec2", "delete-security-group"] for call in calls)
    assert not any(call[:3] in (["aws", "iam", "delete-role"], ["aws", "iam", "delete-role-policy"]) for call in calls)


@pytest.mark.parametrize("action", ["get-role", "get-role-policy"])
@pytest.mark.parametrize("error", ["NoSuchEntity", "AccessDenied"])
def test_teardown_distinguishes_absent_iam_resources_from_errors(
    aws_environment: dict[str, str], action: str, error: str
) -> None:
    aws_environment["TEST_IAM_ERROR_ACTION"] = action
    aws_environment["TEST_IAM_ERROR_CODE"] = error
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert (result.returncode == 0) is (error == "NoSuchEntity"), result.stderr
    calls = _calls(aws_environment)
    assert not any(call[:3] == ["aws", "iam", "delete-role-policy"] for call in calls)
    assert any(call[:3] == ["aws", "iam", "delete-role"] for call in calls) is (
        action == "get-role-policy" and error == "NoSuchEntity"
    )


def test_teardown_preserves_role_used_only_as_association_target(aws_environment: dict[str, str]) -> None:
    aws_environment["TEST_ROLE_SHARED"] = "peer"
    aws_environment["TEST_TARGET_ROLE_ONLY"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr
    assert not any(
        call[:3] in (["aws", "iam", "delete-role"], ["aws", "iam", "delete-role-policy"])
        for call in _calls(aws_environment)
    )


@pytest.mark.parametrize(
    "reason,value", [("TEST_ROLE_UNTAGGED", "true"), ("TEST_ROLE_SHARED", "bench"), ("TEST_ROLE_SHARED", "peer")]
)
def test_teardown_preserves_unowned_or_shared_role(aws_environment: dict[str, str], reason: str, value: str) -> None:
    aws_environment[reason] = value
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr
    assert not any(
        call[:3] in (["aws", "iam", "delete-role"], ["aws", "iam", "delete-role-policy"])
        for call in _calls(aws_environment)
    )


@pytest.mark.parametrize(
    "action",
    ["describe-regions", "list-clusters", "list-pod-identity-associations", "describe-pod-identity-association"],
)
def test_teardown_refuses_identity_deletion_when_usage_is_unknown(aws_environment: dict[str, str], action: str) -> None:
    aws_environment["TEST_IDENTITY_ERROR"] = action
    aws_environment["TEST_ROLE_SHARED"] = "peer"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert not any(
        call[:3] in (["aws", "iam", "delete-role"], ["aws", "iam", "delete-role-policy"])
        for call in _calls(aws_environment)
    )


@pytest.mark.parametrize("eks_present,vpc_override", [(True, ""), (False, "vpc-original"), (False, "")])
def test_teardown_scopes_msk_security_group_to_original_vpc(
    aws_environment: dict[str, str], eks_present: bool, vpc_override: str
) -> None:
    Path(aws_environment["SITE_FILE"]).write_text("kafka: {deployment: managed}\n")
    aws_environment["TEST_MSK_ABSENT"] = "true"
    aws_environment["VPC_ID"] = vpc_override
    if not eks_present:
        aws_environment["TEST_EKS_ABSENT"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    calls = [json.loads(line) for line in Path(aws_environment["CALLS"]).read_text().splitlines()]
    group_calls = [call for call in calls if call[:3] == ["aws", "ec2", "describe-security-groups"]]
    deletes = [call for call in calls if call[:3] == ["aws", "ec2", "delete-security-group"]]
    if eks_present or vpc_override:
        assert result.returncode == 0, result.stderr
        assert len(group_calls) == len(deletes) == 1
        expected_vpc = "vpc-1" if eks_present else vpc_override
        assert f"Name=vpc-id,Values={expected_vpc}" in group_calls[0]
        assert "Name=tag:lakehouse-ingest-bench,Values=true" in group_calls[0]
    else:
        assert result.returncode != 0
        assert "set VPC_ID" in result.stderr
        assert not group_calls and not deletes


@pytest.mark.parametrize("script", ["aws/setup.sh", "aws/teardown.sh", "k8s/stack/setup.sh", "k8s/stack/teardown.sh"])
@pytest.mark.parametrize("configuration", [None, "kafka: {}", "kafka: {deployment: invalid}"])
def test_missing_or_invalid_deployment_fails_before_cloud_access(
    aws_environment: dict[str, str], script: str, configuration: str | None
) -> None:
    source = Path(aws_environment["SITE_FILE"])
    if configuration is None:
        source.unlink()
    else:
        source.write_text(configuration)
    result = subprocess.run(
        ["bash", str(ROOT / "deploy" / script)], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert "--site" in result.stderr, result.stderr
    assert not Path(aws_environment["CALLS"]).exists()


@pytest.mark.parametrize("script", ["setup.sh", "teardown.sh"])
def test_stack_refuses_managed_kafka_before_cloud_access(aws_environment: dict[str, str], script: str) -> None:
    Path(aws_environment["SITE_FILE"]).write_text("kafka: {deployment: managed}")
    result = subprocess.run(
        ["bash", str(ROOT / "deploy/k8s/stack" / script)],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "must set kafka.deployment: in-cluster" in result.stderr
    assert not Path(aws_environment["CALLS"]).exists()


@pytest.mark.parametrize(
    "other_role,other_cluster,lookup_error",
    [
        ("lakehouse-ingest-bench-stack", "us-west-2/bench", ""),
        ("lakehouse-ingest-bench-stack", "us-east-2/peer", ""),
        ("AmazonEKS_EBS_CSI_DriverRole", "us-east-2/peer", ""),
        ("lakehouse-ingest-bench-stack", "us-east-2/peer", "list"),
        ("lakehouse-ingest-bench-stack", "us-east-2/peer", "describe"),
    ],
)
def test_stack_unbind_deletes_role_only_after_checking_remaining_associations(
    tmp_path: Path,
    other_role: str,
    other_cluster: str,
    lookup_error: str,
) -> None:
    """Keep the shared role when an association on any cluster uses it or cannot be checked."""
    if not shutil.which("jq"):
        pytest.skip("requires jq")
    calls = tmp_path / "calls.txt"
    other_region, other_name = other_cluster.split("/")
    stack_role = json.dumps(
        {
            "Role": {
                "Arn": "arn:aws:iam::123456789012:role/lakehouse-ingest-bench-stack",
                "Tags": [{"Key": "lakehouse-ingest-bench", "Value": "true"}],
            }
        }
    )
    result = subprocess.run(
        [
            "bash",
            "-c",
            f'''set -eu
source "{AWS / "_stack_hooks.sh"}"
log() {{ printf '%s\\n' "$*"; }}
die() {{ printf '%s\\n' "$*" >&2; exit 1; }}
aws() {{
    printf '%s\\n' "$*" >>"{calls}"
    case "$*" in
    *list-pod-identity-associations*--namespace*) echo assoc-mine ;;
    "iam get-role "*) echo '{stack_role}' ;;
    "ec2 describe-regions "*) printf 'us-west-2\\tus-east-2\\n' ;;
    "eks list-clusters --region us-west-2 "*) echo bench ;;
    "eks list-clusters --region us-east-2 "*) echo peer ;;
    *"list-pod-identity-associations --region {other_region} --cluster-name {other_name} "*)
        [[ '{lookup_error}' != list ]] || {{ echo 'association listing denied' >&2; return 1; }}
        echo assoc-other ;;
    *list-pod-identity-associations*) echo None ;;
    *describe-pod-identity-association*)
        [[ '{lookup_error}' != describe ]] || {{ echo 'association lookup denied' >&2; return 1; }}
        printf 'arn:aws:iam::123456789012:role/{other_role}\\tNone\\n' ;;
    esac
}}
CLUSTER_NAME=bench
stack_unbind_identity ingest-bench ingest-bench-harness
''',
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    recorded = calls.read_text()
    assert "delete-pod-identity-association --cluster-name bench --association-id assoc-mine" in recorded
    if lookup_error:
        assert result.returncode != 0
        assert "denied" in result.stderr
        assert "iam delete-role" not in recorded
        return
    assert result.returncode == 0, result.stderr
    if other_role == "lakehouse-ingest-bench-stack":
        assert "iam delete-role" not in recorded
        kept = f"keeping iam role lakehouse-ingest-bench-stack: pod identity association {other_cluster}/assoc-other"
        assert kept in result.stdout
    else:
        assert "iam delete-role --role-name lakehouse-ingest-bench-stack" in recorded


def test_catalog_storage_is_baseline_and_independent() -> None:
    manifest = yaml.safe_load((AWS / "k8s" / "catalog-storageclass.yaml").read_text())
    assert manifest["metadata"]["name"] == "ingest-bench-catalog"
    assert manifest["parameters"] == {"type": "gp3", "throughput": "125", "iops": "3000", "encrypted": "true"}
    assert manifest["volumeBindingMode"] == "WaitForFirstConsumer"
    assert manifest["reclaimPolicy"] == "Delete"
    result = subprocess.run(
        [
            "bash",
            "-c",
            f'''set -eu
source "{AWS / "_stack_hooks.sh"}"
log() {{ :; }}
kubectl() {{ printf '%s\\n' "$*"; }}
KUBE_CONTEXT=bench
KAFKA_STORAGE_CLASS=custom-broker-class
stack_delete_storage_class
''',
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "delete storageclass custom-broker-class ingest-bench-catalog --ignore-not-found" in result.stdout


@pytest.mark.parametrize("state", ["present", "absent", "error"])
def test_stack_teardown_requires_known_namespace_presence(aws_environment: dict[str, str], state: str) -> None:
    aws_environment["TEST_NAMESPACE_WARNING"] = "true"
    if state != "present":
        aws_environment[f"TEST_NAMESPACE_{'ABSENT' if state == 'absent' else 'ERROR'}"] = "true"
    result = subprocess.run(
        ["bash", str(ROOT / "deploy/k8s/stack/teardown.sh"), "--yes"],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    calls = _calls(aws_environment)
    assert "credential warning" in result.stderr
    namespace_lookup = next(call for call in calls if call[0] == "kubectl" and "get" in call and "namespace" in call)
    assert "--ignore-not-found" in namespace_lookup
    if state == "error":
        assert result.returncode != 0
        assert "Forbidden" in result.stderr
        assert not any(call[0] == "helmfile" for call in calls)
        assert not any(call[0] == "aws" and call[2].startswith("delete-") for call in calls)
    else:
        assert result.returncode == 0, result.stderr
        releases = [call for call in calls if call[0] == "helmfile" and "destroy" in call]
        assert len(releases) == (0 if state == "absent" else 2)
        assert any(call[0] == "aws" and call[2] == "get-role" for call in calls)


def test_catalog_storage_setup_keeps_broker_tuning_separate() -> None:
    if not shutil.which("envsubst"):
        pytest.skip("requires envsubst")
    result = subprocess.run(
        [
            "bash",
            "-c",
            f'''set -eu
source "{AWS / "_stack_hooks.sh"}"
log() {{ :; }}
kubectl() {{
    printf '%s\\n' '---'
    if [[ ${{@: -1}} == - ]]; then cat; else cat "${{@: -1}}"; fi
}}
KUBE_CONTEXT=bench
KAFKA_VOLUME_THROUGHPUT_MIBS=500
KAFKA_VOLUME_IOPS=12000
stack_storage_class
''',
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    broker, catalog = list(yaml.safe_load_all(result.stdout))
    assert broker["parameters"]["throughput"] == "500"
    assert broker["parameters"]["iops"] == "12000"
    assert catalog["metadata"]["name"] == "ingest-bench-catalog"
    assert catalog["parameters"]["throughput"] == "125"
    assert catalog["parameters"]["iops"] == "3000"


def test_stack_teardown_exports_catalog_class_and_removes_both_classes(
    aws_environment: dict[str, str],
) -> None:
    aws_environment["CLOUD"] = "aws"
    result = subprocess.run(
        ["bash", str(ROOT / "deploy/k8s/stack/teardown.sh"), "--all", "--yes"],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [json.loads(line) for line in Path(aws_environment["CALLS"]).read_text().splitlines()]
    assert [
        "kubectl",
        "--context",
        "bench",
        "delete",
        "storageclass",
        "ingest-bench-kafka",
        "ingest-bench-catalog",
        "--ignore-not-found",
    ] in calls


@pytest.mark.parametrize("catalog", [None, {"props": []}])
def test_external_site_generation_requires_catalog_before_cloud_access(
    aws_environment: dict[str, str], tmp_path: Path, catalog: object
) -> None:
    source: dict[str, object] = {"kafka": {"deployment": "external", "bootstrap_servers": "broker.example:9092"}}
    if catalog is not None:
        source["catalog"] = catalog
    Path(aws_environment["SITE_FILE"]).write_text(yaml.safe_dump(source))
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh"), "--write-site", str(tmp_path / "generated.yaml")],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "catalog.props" in result.stderr
    assert not Path(aws_environment["CALLS"]).exists()
    assert not (tmp_path / "generated.yaml").exists()


@pytest.mark.parametrize("example,managed", [("site.aws.example.yaml", True), ("site.k8s.example.yaml", False)])
def test_teardown_recovers_missing_site_with_example(
    aws_environment: dict[str, str], example: str, managed: bool
) -> None:
    Path(aws_environment["SITE_FILE"]).unlink()
    aws_environment["TEST_MSK_ABSENT"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh"), "--site", str(ROOT / example)],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [json.loads(line) for line in Path(aws_environment["CALLS"]).read_text().splitlines()]
    assert any(call[:3] == ["aws", "kafka", "list-clusters"] for call in calls) == managed


@pytest.mark.parametrize(
    "kafka,catalog",
    [
        ({"bootstrap_servers": "YOUR_KAFKA_BOOTSTRAP"}, {"props": {}}),
        ({"bootstrap_servers": "broker:9092"}, {"props": {"uri": "https://YOUR_CATALOG"}}),
        ({"bootstrap_servers": "broker:9092", "security": {"sasl.password": "YOUR_PASSWORD"}}, {"props": {}}),
    ],
)
def test_external_site_refuses_placeholders_before_cloud_access(
    aws_environment: dict[str, str], tmp_path: Path, kafka: dict[str, object], catalog: dict[str, object]
) -> None:
    source = {"kafka": {"deployment": "external", **kafka}, "catalog": catalog}
    Path(aws_environment["SITE_FILE"]).write_text(yaml.safe_dump(source))
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh"), "--write-site", str(tmp_path / "generated.yaml")],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "YOUR_" in result.stderr
    assert not Path(aws_environment["CALLS"]).exists()
    assert not (tmp_path / "generated.yaml").exists()


def _helm_releases(environment: dict[str, str], action: str) -> list[str]:
    calls = [call for call in _calls(environment) if call[0] == "helm" and action in call]
    return [next(arg for arg in call[call.index(action) + 1 :] if not arg.startswith("-")) for call in calls]


@pytest.mark.parametrize(
    "operators,installed",
    [("flink spark", ["flink-kubernetes-operator", "spark-operator"]), ("spark", ["spark-operator"]), ("", [])],
)
def test_setup_installs_only_listed_operators(
    aws_environment: dict[str, str], operators: str, installed: list[str]
) -> None:
    aws_environment["OPERATORS"] = operators
    aws_environment["TEST_NO_MSK"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert _helm_releases(aws_environment, "upgrade") == installed
    assert ("keeping the existing Flink" in result.stderr) == ("flink" not in operators)
    assert "WARNING: no" not in result.stderr


def test_setup_warns_when_a_kept_operator_is_missing(aws_environment: dict[str, str]) -> None:
    aws_environment["OPERATORS"] = "spark"
    aws_environment["TEST_CRD_ABSENT"] = "true"
    aws_environment["TEST_NO_MSK"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "WARNING: no flinkdeployments.flink.apache.org CRD" in result.stderr


@pytest.mark.parametrize(
    "operators,removed",
    [
        ("flink spark", ["flink-kubernetes-operator", "spark-operator"]),
        ("flink", ["flink-kubernetes-operator"]),
        ("", []),
    ],
)
def test_teardown_all_removes_only_listed_operators(
    aws_environment: dict[str, str], operators: str, removed: list[str]
) -> None:
    aws_environment["OPERATORS"] = operators
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh"), "--all", "--yes"],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert _helm_releases(aws_environment, "uninstall") == removed


@pytest.mark.parametrize("script", ["setup.sh", "teardown.sh"])
def test_unknown_operator_is_rejected_before_cloud_access(aws_environment: dict[str, str], script: str) -> None:
    aws_environment["OPERATORS"] = "flink kafka"
    result = subprocess.run(
        ["bash", str(AWS / script)], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert "OPERATORS may list flink and spark, got 'kafka'" in result.stderr
    assert not Path(aws_environment["CALLS"]).exists()


def _aws_actions(environment: dict[str, str], service: str) -> list[list[str]]:
    return [call[2:] for call in _calls(environment) if call[:2] == ["aws", service]]


def _role_names(environment: dict[str, str], action: str) -> list[str]:
    return [call[call.index("--role-name") + 1] for call in _aws_actions(environment, "iam") if call[0] == action]


def test_setup_uses_the_harness_role_override(aws_environment: dict[str, str]) -> None:
    aws_environment["HARNESS_ROLE_NAME"] = "bench-harness-cluster-b"
    aws_environment["TEST_NO_MSK"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert _role_names(aws_environment, "put-role-policy") == ["bench-harness-cluster-b"]
    associations = [
        call for call in _aws_actions(aws_environment, "eks") if call[0] == "create-pod-identity-association"
    ]
    assert associations
    assert all(call[call.index("--role-arn") + 1].endswith(":role/bench-harness-cluster-b") for call in associations)


def test_setup_creates_an_absent_role_with_the_ownership_tag(aws_environment: dict[str, str]) -> None:
    aws_environment["TEST_ROLE_ABSENT"] = "true"
    aws_environment["TEST_NO_MSK"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
    creates = [call for call in _aws_actions(aws_environment, "iam") if call[0] == "create-role"]
    assert len(creates) == 1 and "Key=lakehouse-ingest-bench,Value=true" in creates[0]
    assert not _role_names(aws_environment, "update-assume-role-policy")


def test_setup_refuses_to_change_an_untagged_role(aws_environment: dict[str, str]) -> None:
    aws_environment["TEST_ROLE_UNTAGGED"] = "true"
    aws_environment["TEST_NO_MSK"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert "HARNESS_ROLE_NAME" in result.stderr
    assert not _role_names(aws_environment, "put-role-policy")
    assert not _role_names(aws_environment, "update-assume-role-policy")


def test_teardown_uses_the_harness_role_override(aws_environment: dict[str, str]) -> None:
    aws_environment["HARNESS_ROLE_NAME"] = "bench-harness-cluster-b"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert _role_names(aws_environment, "delete-role") == ["bench-harness-cluster-b"]


def test_stack_teardown_keeps_an_untagged_stack_role(aws_environment: dict[str, str]) -> None:
    aws_environment["TEST_ROLE_UNTAGGED"] = "true"
    result = subprocess.run(
        ["bash", str(ROOT / "deploy/k8s/stack/teardown.sh"), "--yes"],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert not _role_names(aws_environment, "delete-role")
    assert "ownership tag is absent" in result.stderr


@pytest.mark.parametrize("versioning", ["None", "Enabled"])
def test_setup_leaves_an_unmanaged_bucket_unchanged(aws_environment: dict[str, str], versioning: str) -> None:
    aws_environment["MANAGE_BUCKET"] = "false"
    aws_environment["TEST_BUCKET_VERSIONING"] = versioning
    aws_environment["TEST_NO_MSK"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
    actions = {call[0] for call in _aws_actions(aws_environment, "s3api")}
    assert "head-bucket" in actions
    assert not actions & {"create-bucket", "put-public-access-block", "put-bucket-tagging", "put-bucket-versioning"}
    assert ("noncurrent versions" in result.stderr) == (versioning == "Enabled")


def test_setup_requires_an_unmanaged_bucket_to_exist(aws_environment: dict[str, str]) -> None:
    aws_environment["MANAGE_BUCKET"] = "false"
    aws_environment["TEST_BUCKET_ABSENT"] = "true"
    aws_environment["TEST_NO_MSK"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert "MANAGE_BUCKET=false needs an existing bucket" in result.stderr
    assert not any(call[0] == "create-bucket" for call in _aws_actions(aws_environment, "s3api"))


def test_teardown_all_keeps_an_unmanaged_bucket(aws_environment: dict[str, str]) -> None:
    aws_environment["MANAGE_BUCKET"] = "false"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh"), "--all", "--yes"],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert not _aws_actions(aws_environment, "s3")
    assert not any(call[0] == "delete-bucket" for call in _aws_actions(aws_environment, "s3api"))
    assert "keeping s3://bench-bucket: MANAGE_BUCKET=false" in result.stderr


@pytest.mark.parametrize("script", ["setup.sh", "teardown.sh"])
def test_invalid_manage_bucket_is_rejected_before_cloud_access(aws_environment: dict[str, str], script: str) -> None:
    aws_environment["MANAGE_BUCKET"] = "no"
    result = subprocess.run(
        ["bash", str(AWS / script)], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert "MANAGE_BUCKET must be true or false, got 'no'" in result.stderr
    assert not Path(aws_environment["CALLS"]).exists()


def test_setup_refuses_an_untagged_msk_security_group(aws_environment: dict[str, str]) -> None:
    Path(aws_environment["SITE_FILE"]).write_text("kafka: {deployment: managed}\n")
    aws_environment["TEST_SG_UNTAGGED"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert "Choose an unused MSK_NAME" in result.stderr
    assert not any(call[0] == "authorize-security-group-ingress" for call in _aws_actions(aws_environment, "ec2"))


def test_setup_refuses_to_change_an_untagged_msk_cluster(aws_environment: dict[str, str]) -> None:
    Path(aws_environment["SITE_FILE"]).write_text("kafka: {deployment: managed}\n")
    aws_environment["TEST_MSK_UNTAGGED"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert "MSK cluster lakehouse-ingest-bench exists without" in result.stderr
    assert not any(call[0] == "update-broker-storage" for call in _aws_actions(aws_environment, "kafka"))


def test_teardown_keeps_an_untagged_msk_cluster_and_its_security_group(aws_environment: dict[str, str]) -> None:
    Path(aws_environment["SITE_FILE"]).write_text("kafka: {deployment: managed}\n")
    aws_environment["TEST_MSK_UNTAGGED"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert not any(call[0] == "delete-cluster" for call in _aws_actions(aws_environment, "kafka"))
    assert not any(call[0] == "delete-security-group" for call in _aws_actions(aws_environment, "ec2"))
    assert "keeping MSK cluster lakehouse-ingest-bench" in result.stderr


@pytest.mark.parametrize("untagged", [False, True])
def test_teardown_all_deletes_only_tagged_ecr_repositories(aws_environment: dict[str, str], untagged: bool) -> None:
    if untagged:
        aws_environment["TEST_ECR_UNTAGGED"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh"), "--all", "--yes"],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    deleted = [call for call in _aws_actions(aws_environment, "ecr") if call[0] == "delete-repository"]
    assert len(deleted) == (0 if untagged else 3)


@pytest.mark.parametrize("error", ["RepositoryNotFoundException", "AccessDeniedException", "ExpiredTokenException"])
def test_teardown_distinguishes_absent_ecr_repositories_from_errors(
    aws_environment: dict[str, str], error: str
) -> None:
    aws_environment["TEST_ECR_ERROR"] = error
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh"), "--all", "--yes"],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert not any(call[0] == "delete-repository" for call in _aws_actions(aws_environment, "ecr"))
    if error == "RepositoryNotFoundException":
        assert result.returncode == 0, result.stderr
        assert result.stderr.count("ecr repository") == 3
    else:
        assert result.returncode != 0
        assert error in result.stderr
        assert "could not inspect ecr repository lakehouse-ingest-bench/harness" in result.stderr
        assert "is already gone" not in result.stderr


def test_empty_manage_bucket_is_rejected(aws_environment: dict[str, str]) -> None:
    aws_environment["MANAGE_BUCKET"] = ""
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh"), "--all", "--yes"],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "MANAGE_BUCKET must be true or false, got ''" in result.stderr
    assert not Path(aws_environment["CALLS"]).exists()


@pytest.mark.parametrize("separator", ["\t", "\n", "  "])
def test_operators_accept_any_whitespace(aws_environment: dict[str, str], separator: str) -> None:
    aws_environment["OPERATORS"] = f"flink{separator}spark"
    aws_environment["TEST_NO_MSK"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert _helm_releases(aws_environment, "upgrade") == ["flink-kubernetes-operator", "spark-operator"]


@pytest.mark.parametrize("script", ["setup.sh", "teardown.sh"])
def test_operator_patterns_are_not_expanded_against_files(
    aws_environment: dict[str, str], tmp_path: Path, script: str
) -> None:
    workdir = tmp_path / "work"
    (workdir / "flink").mkdir(parents=True)
    aws_environment["OPERATORS"] = "fl*"
    result = subprocess.run(
        ["bash", str(AWS / script)], env=aws_environment, cwd=workdir, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert "got 'fl*'" in result.stderr
    assert not Path(aws_environment["CALLS"]).exists()
