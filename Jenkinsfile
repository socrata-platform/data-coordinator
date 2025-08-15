@Library('socrata-pipeline-library@sarahs/sbt-library-publishing') _

commonPipeline(
  abortPrevious: false,
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
      deploymentEcosystem: 'marathon-mesos',
      marathonInstanceNamePattern: 'data-coordinator*',
      paths: [
        dockerBuildContext: 'coordinator/docker'
      ],
      type: 'service',
    ],
    [
      name: 'coordinator-external',
      paths: [
        libraryBuildContext: 'coordinator-external',
        libraryVersion: 'version.sbt',
      ],
      type: 'library',
    ],
  ],
  teamsChannelWebhookId: 'WORKFLOW_EGRESS_AUTOMATION',
)
// TODO: support sbtbuild.setRunITTest(true) = sbt it:test
