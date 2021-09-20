export DOCKER_BUILDKIT=1

docker image build . -t bipbop:latest

docker run --env-file ./db_login.env --rm bipbop:latest
