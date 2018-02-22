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
				sh 'mvn -U clean install -DskipTests=false'
			}
		}
		stage('Build Docker') {
			steps {
				sh "./build-docker.sh ${env.BRANCH_NAME}-latest"
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
