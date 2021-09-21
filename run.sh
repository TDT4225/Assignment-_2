export DOCKER_BUILDKIT=1

docker image build . -t bipbop:latest

docker run --env-file ./db_login.env  --mount type=bind,source="$(pwd)"/datasett,target=/datasett --rm bipbop:latest
