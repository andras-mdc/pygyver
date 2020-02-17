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
	docker pull ${IMAGE_REGISTRY}/waxit:${CACHE_TAG} || true
	docker pull ${IMAGE_REGISTRY}/waxit:latest || true

build-ci: 
	docker-compose build waxit
	docker tag ${IMAGE_REGISTRY}/waxit:${DOCKER_TAG} ${IMAGE_REGISTRY}/waxit:${CACHE_TAG}
	docker tag ${IMAGE_REGISTRY}/waxit:${DOCKER_TAG} ${IMAGE_REGISTRY}/waxit:latest

run-tests-ci:
	docker-compose -p ${TESTNAME} run --no-deps --name ${TESTNAME}${DOCKER_TAG} waxit-tests

push-ci:
	docker-compose push waxit

#################################################################
### FOR LOCAL DEVELOPMENT
#################################################################

# run 'source load-local-vars.sh' to load env vars prior to local development

build: 
	docker pull ${IMAGE_REGISTRY}/py-base:${BASE_IMAGE_GIT_TAG} || true
	docker pull ${IMAGE_REGISTRY}/waxit:latest || true
	docker-compose build waxit-local

run-tests:
	docker-compose run waxit-tests-local

run-waxit:
ifdef ENTRYPOINT
	docker-compose run waxit-local $(ENTRYPOINT)
else
	echo "No Entrypoint Specified"
endif
