pipeline {
	agent any
  	tools {
      		maven 'mvn 3.5'
  	}
	triggers {
  		upstream(upstreamProjects: "qwanda-services", threshold: hudson.model.Result.SUCCESS)
	}
	stages {
		stage('Build') {
			steps {
				sh './build.sh'
				sh 'mvn clean install -DskipTests=true'
			}
		}
		stage('Deploy') {
      			when { branch 'master'}
			steps {
				sh 'echo Deploying...'
			}
		}
		stage('Done') {
			steps {
				sh 'echo Slacking'
			}
		}
	}
	post {
		always {
	    deleteDir()
	  }
	}
}

pipeline {
	agent any
  tools {
      maven 'mvn 3.5'
  }
	stages {
		stage ('Clone') {
		  steps {
				cleanWs()
		  	checkout scm
				script {
					releaseVersion = sh(returnStdout: true, script: 'git fetch -t && git describe --abbrev=0 --tags')
        }
		  }
		}
		stage('Build') {
			steps {
				sh './build.sh'
				sh 'mvn clean install -U -DskipTests=false'
			}
		}
		stage('Push to Nexus') {
			steps {
				nexusPublisher nexusInstanceId: 'OUTCOME_NEXUS', nexusRepositoryId: 'life.genny', packages: [[$class: 'MavenPackage', mavenAssetList: [[classifier: '', extension: '', filePath: 'target/qwanda-services.jar']], mavenCoordinate: [artifactId: 'qwanda-services', groupId: 'life.genny', packaging: 'jar', version: '1.0.7']]]
			}
		}
	}
	post {
		success {
			withCredentials([string(credentialsId: 'e78cedc6-d1c0-4ff4-9fbb-fb65f7190c5d', variable: 'SLACK_WEBHOOK')]) {
				sh "curl -X POST -H 'Content-type: application/json' --data '{\"text\":\"gennyproject/qwanda-utils:${env.BRANCH_NAME}-${env.BUILD_NUMBER} successfully built! :goodstuff:\"}' ${SLACK_WEBHOOK}"
			}
		}
		failure {
			withCredentials([string(credentialsId: 'e78cedc6-d1c0-4ff4-9fbb-fb65f7190c5d', variable: 'SLACK_WEBHOOK')]) {
				sh "curl -X POST -H 'Content-type: application/json' --data '{\"text\":\"🚨🚨 gennyproject/qwanda-utils:${env.BRANCH_NAME}-${env.BUILD_NUMBER} failed to build/push! 🚨🚨\"}' ${SLACK_WEBHOOK}"
			}
		}
	}
}
