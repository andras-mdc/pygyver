#!groovy
import hudson.AbortException
import org.yaml.snakeyaml.Yaml

if(env.BRANCH_NAME == "master" || env.CHANGE_ID) {

  node('aws-ecr') {
    properties([disableConcurrentBuilds()])

    TOWER_PROD_ID = 13189
    TOWER_UAT_ID = 13201

    stage("checkout") {
        cleanWs()
        checkout scm
    }

    env.CLEANUP = true

    def ENV = 'test'
    def PY_VERSION = '3.8.1'
    def BASE_IMAGE_REPO = 'git@github.com:madedotcom/analytics-python-docker-base.git'
    def IMAGE_REGISTRY="087665217675.dkr.ecr.eu-west-1.amazonaws.com/analytics"

    stage('docker tags') {
        //create docker tag
        def SHORT_HASH = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
        sh "echo '${env.BRANCH_NAME.replaceAll(/\//, '.')}-${BUILD_NUMBER}-${SHORT_HASH}-${PY_VERSION}' > .git/docker-tag"
        def DOCKER_TAG = readFile('.git/docker-tag').trim()
        currentBuild.displayName = DOCKER_TAG
        
        //get base image git tag
        def BASE_IMAGE_GIT_TAG = sshagent(credentials: ['Github-SSH']){sh(returnStdout: true, script:'git ls-remote --tags ' + "${BASE_IMAGE_REPO}" + ' | grep '+ "${PY_VERSION}" +' | cut -d "/" -f 3 | sort -nr -b -t - -k 2 | awk "NR==1"')}
        sh("echo 'Using py base image tag: ${BASE_IMAGE_GIT_TAG}'")
        sh "echo '${BASE_IMAGE_GIT_TAG}' > .git/base-image-git-tag"
    }

    def DOCKER_TAG = readFile('.git/docker-tag').trim()
    def BASE_IMAGE_GIT_TAG = readFile('.git/base-image-git-tag').trim()

    stage('generate secret files') {
      withCredentials([
        file(credentialsId: 'sandbox_access_token_json', variable: 'GOOGLE_APPLICATION_CREDENTIALS'),
        string(credentialsId: 'analytics-email-hash-salt', variable: 'EMAIL_HASH_SALT'),
        [$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'madecom-bi-sandbox-aws', accessKeyVariable: 'AWS_ACCESS_KEY_ID', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'],
      ]){
        sh "cp $GOOGLE_APPLICATION_CREDENTIALS ."
        sh "consul-template -once -template input.ctmpl:vars/env/test/test-secrets.env"
      }
    }

    withEnv([
      'ENV=' + "${ENV}",
      'PY_VERSION=' + "${PY_VERSION}",
      'IMAGE_REGISTRY=' + "${IMAGE_REGISTRY}",
      'DOCKER_TAG=' + "${DOCKER_TAG}",
      'CACHE_TAG=' + "${env.BRANCH_NAME}",
      'BASE_IMAGE_GIT_TAG=' + "${BASE_IMAGE_GIT_TAG}"]) {
      try {
        withCredentials([
          string(credentialsId: 'analytics_rmade_service_account_token', variable: 'GITHUB_PAT')]) {

            stage("clean"){
              sh "make clean"
            }

            stage ("build") {
              sh "make pull-ci"
              sh "DOCKER_TAG=${DOCKER_TAG} make build-ci"
            }

            stage ("push cache image") {
                sh "DOCKER_TAG=${CACHE_TAG} make push-ci"
            }

            lock('pygyver-tests') {

              stage('parallel tests') {
                buildStages = prepareBuildStages([])
                for (builds in buildStages) {
                    parallel(builds)
                }
              }
              
              stage('serial tests') {
                  buildStages = prepareBuildStages(['pygyver-tests'])
                  println("Serial tests initialised")
                  for (builds in buildStages) {
                      for (build in builds.values()) {
                          build.call()
                      }
                  }
              }
            }

            stage ("push latest and docker_tag images") {
                sh "DOCKER_TAG=${DOCKER_TAG} make push-ci"
                sh "DOCKER_TAG=latest make push-ci"
            }  
        }

        // stage ("push release git tag") {
        //   if(env.BRANCH_NAME == "master") {
        //     sh "git tag ${DOCKER_TAG}" 
        //     pushGitTag()
        //   }
        //   else {
        //     echo "Tag push to Github skipped because not on master branch"
        //   }
        // }
      }
      finally {
        stage ("cleanup") {
          if (env.CLEANUP) {
            sh "make clean"
          }
        }
      }
    }
  }
}   

////////////////////////
//Functions
///////////////////////

// Create List of build stages to suit
def prepareBuildStages(stage_list) {
  def buildList = []
  def buildStages = [:]
  for (name in stage_list ) {
    def n = "${name}"
    buildStages.put(n, prepareOneBuildStage(n))
  }
  buildList.add(buildStages)
  return buildList 
}

// Create a single stage
def prepareOneBuildStage(String name) {
  return {
    stage("${name}") {
      println("Building ${name}")
      def dockerTag = readFile('.git/docker-tag').trim()
      sh "DOCKER_TAG=${dockerTag} TESTNAME=${name} make run-tests-ci"
      def containerName = "${name}${dockerTag}"
      def container = sh(returnStdout: true, script:"docker ps -a -q --filter name=${containerName}").trim()
      sh(returnStdout: true, script: "docker cp ${container}:/code/src/test-out-${name}.xml .")
      junit "test-out-${name}.xml"
    }
  }
} 

// // Push git tag
// def pushGitTag(){
//   sshagent(credentials: ['Github-SSH']){
//     sh('git push -f --tags') 
//   }
// }