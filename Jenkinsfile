@Library('socrata-pipeline-library@sarahs/sbt-library-publishing') _

commonPipeline(
    defaultBuildWorker: 'build-worker-pg13',
    jobName: 'data-coordinator',
    language: 'scala',
    languageOptions: [
        isMultiProjectRepository: true,
    ],
    numberOfBuildsToKeep: 50,
    projects: [
  /*
        [
            name: 'data-coordinator', //TODO: project and folder need to be renamed
            compiled: true,
            deploymentEcosystem: 'marathon-mesos',
            docker: [
                buildContext: 'coordinator/docker'
            ],
            marathonInstanceNamePattern: 'data-coordinator*',
            type: 'service',
        ],
  */
        [
            name: 'coordinator-external',
            paths: [
                buildContext: 'coordinator-external',
                version: 'version.sbt',
            ],
            type: 'library',
        ],
    ],
    teamsChannelWebhookId: 'WORKFLOW_EGRESS_AUTOMATION',
)
// TODO: support sbtbuild.setRunITTest(true) = sbt it:test
