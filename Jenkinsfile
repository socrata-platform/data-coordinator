// Set up the libraries
@Library('socrata-pipeline-library@sarahs/EN-78944/automate-hotfix-branch-creation-and-cleanup')

import com.socrata.ReleaseMetadataService
def rmsSupportedEnvironment = com.socrata.ReleaseMetadataService.SupportedEnvironment

// set up service and project variables
String service = 'data-coordinator'
String project_wd = 'coordinator'
boolean isPr = env.CHANGE_ID != null
boolean isHotfix = isHotfixBranch(env.BRANCH_NAME)
boolean skip = false
String lastStage
boolean publishLibrary = false

// instanciate libraries
def sbtbuild = new com.socrata.SBTBuild(steps, service, project_wd)
def dockerize = new com.socrata.Dockerize(steps, service, BUILD_NUMBER)
def releaseTag = new com.socrata.ReleaseTag(steps, service)
def semVerTag = new com.socrata.SemVerTag(steps)

pipeline {
  options {
    ansiColor('xterm')
    buildDiscarder(logRotator(numToKeepStr: '50'))
    timeout(time: 20, unit: 'MINUTES')
  }
  parameters {
    string(name: 'AGENT', defaultValue: 'build-worker-pg13', description: 'Which build agent to use?')
    string(name: 'BRANCH_SPECIFIER', defaultValue: 'origin/main', description: 'Use this branch for building the artifact.')
    booleanParam(name: 'RELEASE_BUILD', defaultValue: false, description: 'Are we building a release candidate?')
    booleanParam(name: 'RELEASE_DRY_RUN', defaultValue: false, description: 'To test out the release build.')
    string(name: 'RELEASE_NAME', description: 'For release builds, the release name which is used in the git tag and the build id.')
  }
  agent {
    label params.AGENT
  }
  environment {
    SCALA_VERSION = '2.12'
    DEPLOY_PATTERN = "${service}*"
    WEBHOOK_ID = 'WORKFLOW_IQ'
  }
  stages {
    stage('Check for Version Change') {
      when {
        branch 'main'
        not { expression { params.RELEASE_BUILD } }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
         // Checkout to fetch tags because the default checkout does not include tags
          // Fetching the tags without uisng checkout will confuse Jenkins about the git branch
          semVerTag.checkout('main')
          publishLibrary = haveCertainFilesChanged(filePaths: ['version.sbt'])
        }
      }
    }
    stage('Publish Library') {
      when {
        branch 'main'
        not { expression { params.RELEASE_BUILD } }
        expression { publishLibrary }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          skip = true
          semVerTag.checkoutClosestTag()
          // Build & Publish
          sbtbuild.setRunITTest(true)
          sbtbuild.setNoSubproject(true)
          sbtbuild.setScalaVersion(env.SCALA_VERSION)
          sbtbuild.setPublish(true)
          sbtbuild.setBuildType("library")
          sbtbuild.build()
        }
      }
    }
    stage('Hotfix Tag') {
      when {
        expression { isHotfix }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          if (releaseTag.noCommitsOnHotfixBranch(env.BRANCH_NAME)) {
            skip = true
            echo "SKIP: Skipping the rest of the build because there are no commits on the hotfix branch yet"
          } else {
            env.CURRENT_RELEASE_NAME = releaseTag.getReleaseName(env.BRANCH_NAME)
            env.HOTFIX_NAME = releaseTag.getHotfixName(env.CURRENT_RELEASE_NAME)
          }
        }
      }
    }
    stage('Build') {
      when {
        allOf {
          not { expression { return params.PUBLISH } }
          not { expression { skip } }
        }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          sbtbuild.setRunITTest(true)
          sbtbuild.setNoSubproject(true)
          sbtbuild.setPublish(false)
          sbtbuild.setScalaVersion(env.SCALA_VERSION)
          sbtbuild.setSrcJar("coordinator/target/coordinator-assembly.jar")
          sbtbuild.build()
        }
      }
    }
    stage('Docker Build') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { skip } }
          not { expression { return params.PUBLISH } }
        }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          if (params.RELEASE_BUILD || isHotfix) {
            env.VERSION = (isHotfix) ? env.HOTFIX_NAME : params.RELEASE_NAME
            env.DOCKER_TAG = dockerize.dockerBuildWithSpecificTag(
              tag: env.VERSION,
              path: sbtbuild.getDockerPath(),
              artifacts: [sbtbuild.getDockerArtifact()]
            )
          } else {
            env.DOCKER_TAG = dockerize.dockerBuildWithDefaultTag(
              version: 'STAGING',
              sha: env.GIT_COMMIT,
              path: sbtbuild.getDockerPath(),
              artifacts: [sbtbuild.getDockerArtifact()]
            )
          }
        }
      }
      post {
        success {
          script {
            if (isHotfix) {
              env.GIT_TAG = releaseTag.create(env.HOTFIX_NAME)
            } else if (params.RELEASE_BUILD) {
              env.GIT_TAG = releaseTag.getFormattedTag(params.RELEASE_NAME)
              if (releaseTag.doesReleaseTagExist(params.RELEASE_NAME)) {
                echo "REBUILD: Tag ${env.GIT_TAG} already exists"
              }
              if (params.RELEASE_DRY_RUN) {
                echo "DRY RUN: Would have created ${env.GIT_TAG} and pushed it to the repo"
                currentBuild.description = "${service}:${params.RELEASE_NAME} - DRY RUN"
              }
              releaseTag.create(params.RELEASE_NAME)
            }
            if (params.RELEASE_BUILD) {
              releaseTag.createHotfixBranch(service, params.RELEASE_NAME, params.RELEASE_DRY_RUN)
              releaseTag.cleanUpHotfixBranch(service, params.RELEASE_NAME, params.RELEASE_DRY_RUN)
            }
          }
        }
      }
    }
    stage('Publish Image') {
      when {
        allOf {
          not { expression { isPr } }
          not { expression { skip } }
          not { expression { return params.PUBLISH } }
          not { expression { return params.RELEASE_BUILD && params.RELEASE_DRY_RUN } }
        }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          if (isHotfix || params.RELEASE_BUILD) {
            env.BUILD_ID = dockerize.publish(sourceTag: env.DOCKER_TAG)
          } else {
            env.BUILD_ID = dockerize.publish(
              sourceTag: env.DOCKER_TAG,
              environments: ['internal']
            )
          }
          currentBuild.description = env.BUILD_ID
        }
      }
      post {
        success {
          script {
            if (isHotfix || params.RELEASE_BUILD) {
              env.PURPOSE = (isHotfix) ? 'hotfix' : 'initial'
              env.RELEASE_ID = (isHotfix) ? env.CURRENT_RELEASE_NAME : params.RELEASE_NAME
              Map buildInfo = [
                "project_id": service,
                "build_id": env.BUILD_ID,
                "release_id": env.RELEASE_ID,
                "git_tag": env.GIT_TAG,
                "purpose": env.PURPOSE,
              ]
              createBuild(
                buildInfo,
                rmsSupportedEnvironment.production
              )
            }
          }
        }
      }
    }
    stage('Deploy') {
      when {
        not { expression { isPr } }
        not { expression { skip } }
        not { expression { return params.RELEASE_BUILD } }
        not { expression { return params.PUBLISH } }
      }
      steps {
        script {
          lastStage = env.STAGE_NAME
          env.ENVIRONMENT = (isHotfix) ? 'rc' : 'staging'
          marathonDeploy(
            serviceName: env.DEPLOY_PATTERN,
            tag: env.BUILD_ID,
            environment: env.ENVIRONMENT
          )
          // While working on migrating from marathon to ECS, we are keeping the tagged images up to date
          // Once the migration is done, we will remove the marathonDeploy and leave in place this publish which triggers the ECS deployment
          env.TARGET_DEPLOY_TAG = (env.ENVIRONMENT == 'rc') ? 'rc' : 'latest'
          dockerize.publish(
            sourceTag: env.DOCKER_TAG,
            targetTag: env.TARGET_DEPLOY_TAG,
            environments: ['internal']
          )
        }
      }
      post {
        success {
          script {
            if (isHotfix) {
              Map deployInfo = [
                "build_id": env.BUILD_ID,
                "environment": env.ENVIRONMENT,
              ]
              createDeployment(
                deployInfo,
                rmsSupportedEnvironment.production
              )
            }
          }
        }
      }
    }
  }
  post {
    failure {
      script {
        boolean buildingMain = (env.JOB_NAME.contains("${service}/main"))
        if (buildingMain) {
          teamsWorkflowMessage(
            message: "[${currentBuild.fullDisplayName}](${env.BUILD_URL}) has failed in stage ${lastStage}",
            workflowCredentialID: WEBHOOK_ID
          )
        }
      }
    }
  }
}
