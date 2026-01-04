pipeline {
  agent any

  environment {
    PYTHON = 'python'
    PIP = 'pip'
    // Optional: override DB path and URL for API/CSV
    TRANSPORT_DB_URL = "${env.TRANSPORT_DB_URL ?: ''}"
    TRAFFIC_API_URL = "${env.TRAFFIC_API_URL ?: ''}"
    TRANSPORT_CSV_URL = "${env.TRANSPORT_CSV_URL ?: ''}"
    DATA_GIT_PULL = "true"
    ETL_LOG_LEVEL = "INFO"

    // Email (configure Jenkins credentials or pass via job params)
    SMTP_HOST = "${env.SMTP_HOST ?: ''}"
    SMTP_PORT = "${env.SMTP_PORT ?: '587'}"
    SMTP_USER = "${env.SMTP_USER ?: ''}"
    SMTP_PASS = "${env.SMTP_PASS ?: ''}"
    ALERT_EMAIL_FROM = "${env.ALERT_EMAIL_FROM ?: ''}"
    ALERT_EMAIL_TO = "${env.ALERT_EMAIL_TO ?: ''}"

    // Deployment directory for dashboard (static location on agent or network share)
    DASH_DEPLOY_DIR = "${env.DASH_DEPLOY_DIR ?: ''}"
  }

  options {
    timestamps()
    ansiColor('xterm')
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Install Dependencies') {
      steps {
        bat "${PYTHON} -m pip install --upgrade pip"
        bat "${PYTHON} -m pip install -r requirements.txt"
      }
    }

    stage('Run ETL') {
      steps {
        bat "${PYTHON} scripts/etl_pipeline.py"
      }
    }

    stage('Package Dashboard') {
      when { expression { return env.DASH_DEPLOY_DIR?.trim() } }
      steps {
        powershell "New-Item -ItemType Directory -Force -Path \"${DASH_DEPLOY_DIR}\" | Out-Null"
        powershell "Copy-Item -Recurse -Force app \"${DASH_DEPLOY_DIR}\""
      }
    }

    stage('Smoke Test Dashboard Load') {
      steps {
        bat "${PYTHON} -c \"import os,sys; db=os.path.join(os.getcwd(),'data_pipeline.db'); print('DB path:', db); sys.exit(0 if os.path.exists(db) else 1)\""
      }
    }
  }

  post {
    always {
      archiveArtifacts artifacts: 'logs/**, reports/**', allowEmptyArchive: true
    }
    failure {
      script {
        def msg = "ETL/Dashboard pipeline failed on ${env.JOB_NAME} #${env.BUILD_NUMBER}. See console and archived logs."
        try {
          emailext (
            subject: "Jenkins Failure: ${env.JOB_NAME} #${env.BUILD_NUMBER}",
            body: msg,
            to: env.ALERT_EMAIL_TO ?: '',
            from: env.ALERT_EMAIL_FROM ?: ''
          )
        } catch (err) {
          echo "Email extension not configured or failed: ${err}"
        }
      }
    }
  }
}
