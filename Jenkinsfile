// Set up the libraries
@Library('socrata-pipeline-library')


// set up service and project variables
def service = "data-coordinator"
def project_wd = "coordinator"
def deploy_service_pattern = "data-coordinator*"
def deploy_environment = "staging"
def default_branch_specifier = "origin/master"
def scala_version = "2.12"

def service_sha = env.GIT_COMMIT

// variables that determine which stages we run based on what triggered the job
def boolean stage_cut = false
def boolean stage_build = false
def boolean stage_dockerize = false
def boolean stage_deploy = false

// instanciate libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service, project_wd)
def dockerize = new com.socrata.Dockerize(steps, service, BUILD_NUMBER)
def deploy = new com.socrata.MarathonDeploy(steps)

pipeline {
  options {
    ansiColor('xterm')
  }
  parameters {
    booleanParam(name: 'RELEASE_CUT', defaultValue: false, description: 'Are we cutting a new release candidate?')
    booleanParam(name: 'FORCE_BUILD', defaultValue: false, description: 'Force build from latest tag if sbt release needed to be run between cuts')
    string(name: 'AGENT', defaultValue: 'build-worker', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: default_branch_specifier, description: 'Use this branch for building the artifact.')
  }
  agent {
    label params.AGENT
  }
  environment {
    PATH = "${WORKER_PATH}"
  }

  stages {
    stage('Setup') {
      steps {
        script {
          // check to see if we want to use a non-standard branch and check out the repo
          if (params.BRANCH_SPECIFIER == default_branch_specifier) {
            checkout scm
          } else {
            def scmRepoUrl = scm.getUserRemoteConfigs()[0].getUrl()
            checkout ([
              $class: 'GitSCM',
              branches: [[name: params.BRANCH_SPECIFIER ]],
              userRemoteConfigs: [[ url: scmRepoUrl ]]
            ])
          }

          // set the service sha to what was checked out (GIT_COMMIT isn't always set)
          service_sha = sh(returnStdout: true, script: "git rev-parse HEAD").trim()

          // determine what triggered the build and what stages need to be run

          if (params.RELEASE_CUT == true) { // RELEASE_CUT parameter was set by a cut job
            stage_cut = true  // other stages will be turned on in the cut step as needed
            deploy_environment = "rc"
          }
          else if (env.CHANGE_ID != null) { // we're running a PR builder
            stage_build = true
            // Query-coordinator just uses sbt clean/compile/test for testing (these are the default sbt build operations)
            // If your service has additional testing, you may want to use the Test stage to implement that and uncomment the next line:
            // stage_test = true
          }
          else if (BRANCH_NAME == "master") { // we're running a build on master branch to deploy to staging
            stage_build = true
            stage_dockerize = true
            stage_deploy = true
          }
          else {
            // we're not sure what we're doing...
            echo "Unknown build trigger - Exiting as Failure"
            currentBuild.result = 'FAILURE'
            return
          }
        }
      }
    }
    stage('Cut') {
      when { expression { stage_cut } }
      steps {
        script {
          def cutNeeded = false

          // get a list of all files changes since the last tag
          files = sh(returnStdout: true, script: "git diff --name-only HEAD `git describe --match \"v*\" --abbrev=0`").trim()
          echo "Files changed:\n${files}"

          if (files == 'version.sbt') {
            // Build anyway using latest tag - needed if sbt release had to be run between cuts
            // This parameter will need to be set by the cut job in Jenkins
            if(params.FORCE_BUILD) {
              cutNeeded = true
            }
            else {
              echo "No build needed, skipping subsequent steps"
            }
          }
          else {
            echo 'Running sbt-release'

            // The git config setup required for your project prior to running 'sbt release with-defaults' may vary:
            sh(returnStdout: true, script: "git config remote.origin.fetch +refs/heads/*:refs/remotes/origin/*")
            sh(returnStdout: true, script: "git config branch.master.remote origin")
            sh(returnStdout: true, script: "git config branch.master.merge refs/heads/master")

            // increasing meta space size to avoid build errors during release step
            javaopts = "JAVA_OPTS=-XX:MaxMetaspaceSize=512m"

            withEnv([javaopts]) {
              echo sh(returnStdout: true, script: "echo y | sbt \"release with-defaults\"")
            }

            cutNeeded = true
          }

          if(cutNeeded == true) {
            echo 'Getting release tag'
            release_tag = sh(returnStdout: true, script: "git describe --abbrev=0 --match \"v*\"").trim()
            branchSpecifier = "refs/tags/${release_tag}"
            echo branchSpecifier

            // checkout the tag so we're performing subsequent actions on it
            sh "git checkout ${branchSpecifier}"

            // set the service_sha to the current tag because it might not be the same as env.GIT_COMMIT
            service_sha = sh(returnStdout: true, script: "git rev-parse HEAD").trim()

            // set later stages to run since we're cutting
            stage_build = true
            stage_dockerize = true
            stage_deploy = true
          }
        }
      }
    }
    stage('Build') {
      when { expression { stage_build } }
      steps {
        script {
          // perform any needed modifiers on the build parameters here
          sbtbuild.setRunITTest(true)
          sbtbuild.setNoSubproject(true)
          sbtbuild.setScalaVersion(scala_version)

          // build
          echo "Building sbt project..."
          sbtbuild.build()

          // If we're cutting, also publish libraries if they haven't already been published for this version
          if(stage_cut == true) {
            result = sh(returnStdout: true, script: "#!/bin/sh -e\ncurl -s -o /dev/null -w \"%{http_code}\" -u ${ARTIFACTORY_USER}:${ARTIFACTORY_PASSWORD} https://repo.socrata.com/artifactory/libs-release/com/socrata/coordinatorlib_${scala_version}/${sbtbuild.getServiceVersion()}/coordinatorlib_${scala_version}-${sbtbuild.getServiceVersion()}.pom").trim()
            echo "Check for published artifact result: [${result}]"
            if(result == "404") {
              echo sh(returnStdout: true, script: "sbt +publish")
            }
          }
        }
      }
    }
    stage('Dockerize') {
      when { expression { stage_dockerize } }
      steps {
        script {
          echo "Building docker container..."
          dockerize.docker_build(sbtbuild.getServiceVersion(), service_sha, sbtbuild.getDockerPath(), sbtbuild.getDockerArtifact())
        }
      }
    }
    stage('Deploy') {
      when { expression { stage_deploy } }
      steps {
        script {
          // Checkout and run bundle install in the apps-marathon repo
          deploy.checkoutAndInstall()

          // deploy the service to the specified environment
          deploy.deploy(deploy_service_pattern, deploy_environment, dockerize.getDeployTag())
        }
      }
    }
  }
}
