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
        [
            name: 'data-coordinator',
            compiled: true,
            deploymentEcosystem: 'ecs',
            docker: [
                buildContext: 'data-coordinator/docker'
            ],
            language: 'scala',
            type: 'service',
        ],
        [
            name: 'coordinator-external',
            compiled: true,
            language: 'scala',
            paths: [
                buildContext: 'coordinator-external',
                versionFile: 'version.sbt',
            ],
            type: 'library',
        ],
    ],
    teamsChannelWebhookId: 'WORKFLOW_EGRESS_AUTOMATION',
)
