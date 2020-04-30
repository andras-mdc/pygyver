#################################################################
### CI-CD for travis
#################################################################

travis: tox

init:
	pip install -r requirements.txt
	pip install -e .

tox:
	tox

#################################################################
### GENERIC
#################################################################

clean:
	docker-compose down --rmi all || true

#################################################################
### FOR CI/CD (JENKINS, CLOUDBUILD)
#################################################################

pull-ci:
	docker pull ${IMAGE_REGISTRY}/py-base:${BASE_IMAGE_GIT_TAG} || true
	docker pull ${IMAGE_REGISTRY}/pygyver:${CACHE_TAG} || true
	docker pull ${IMAGE_REGISTRY}/pygyver:latest || true

build-ci: 
	docker-compose build pygyver
	docker tag ${IMAGE_REGISTRY}/pygyver:${DOCKER_TAG} ${IMAGE_REGISTRY}/pygyver:${CACHE_TAG}
	docker tag ${IMAGE_REGISTRY}/pygyver:${DOCKER_TAG} ${IMAGE_REGISTRY}/pygyver:latest

run-tests-ci:
	docker-compose -p ${TESTNAME} run --no-deps --name ${TESTNAME}${DOCKER_TAG} pygyver-tests

push-ci:
	docker-compose push pygyver

#################################################################
### FOR LOCAL DEVELOPMENT
#################################################################

# run 'source load-local-vars.sh' to load env vars prior to local development

build: 
	docker pull ${IMAGE_REGISTRY}/py-base:${BASE_IMAGE_GIT_TAG} || true
	docker pull ${IMAGE_REGISTRY}/pygyver:latest || true
	docker-compose build pygyver-local

run-tests:
	docker-compose run pygyver-tests-local

run-pygyver:
ifdef ENTRYPOINT
	docker-compose run pygyver-local $(ENTRYPOINT)
else
	echo "No Entrypoint Specified"
endif
