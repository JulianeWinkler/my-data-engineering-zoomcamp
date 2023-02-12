from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from etl_web_to_gcs_parameterized import etl_parent_flow


docker_block = DockerContainer.load("zoomcamp")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='docker_flow_etl',
    infrastructure=docker_block
)

if __name__ == '__main__':
    docker_dep.apply()