pipeline {
    agent {
        docker {
            label 'main'
            image docker.build("storj-ci", "--pull git://github.com/storj/ci.git#main").id
            args '-u root:root --cap-add SYS_PTRACE -v "/tmp/gomod":/go/pkg/mod'
        }
    }
    options {
          timeout(time: 26, unit: 'MINUTES')
    }
    environment {
        NPM_CONFIG_CACHE = '/tmp/npm/cache'
        COCKROACH_MEMPROF_INTERVAL=0
    }
    stages {
        stage('Build') {
            steps {
                checkout scm

                sh 'mkdir -p .build'

                sh 'service postgresql start'

                dir(".build") {
                    sh 'cockroach start-single-node --insecure --store=\'/tmp/crdb\' --listen-addr=localhost:26257 --http-addr=localhost:8080 --cache 512MiB --max-sql-memory 512MiB --background'
                }
            }
        }

        stage('Verification') {
            parallel {
                stage('Lint') {
                    steps {
                        sh 'check-copyright'
                        sh 'check-large-files'
                        sh 'check-imports ./...'
                        sh 'check-peer-constraints'
                        sh 'storj-protobuf --protoc=$HOME/protoc/bin/protoc lint'
                        sh 'storj-protobuf --protoc=$HOME/protoc/bin/protoc check-lock'
                        sh 'check-atomic-align ./...'
                        sh 'check-monkit ./...'
                        sh 'check-errs ./...'
                        sh './scripts/check-dependencies.sh'
                        sh 'staticcheck ./...'
                        sh 'golangci-lint --config /go/ci/.golangci.yml -j=2 run'
                        sh 'go-licenses check ./...'
                        sh './scripts/check-libuplink-size.sh'

                        dir("testsuite") {
                            sh 'check-imports ./...'
                            sh 'check-atomic-align ./...'
                            sh 'check-monkit ./...'
                            sh 'check-errs ./...'
                            sh 'staticcheck ./...'
                            sh 'golangci-lint --config /go/ci/.golangci.yml -j=2 run'
                        }
                    }
                }

                stage('Tests') {
                    environment {
                        COVERFLAGS = "${ env.BRANCH_NAME != 'main' ? '' : '-coverprofile=.build/coverprofile -coverpkg=./...'}"
                    }
                    steps {
                        sh 'go vet ./...'
                        sh 'go test -parallel 4 -p 6 -vet=off $COVERFLAGS -timeout 20m -json -race ./... 2>&1 | tee .build/tests.json | xunit -out .build/tests.xml'
                        // TODO enable this later
                        // sh 'check-clean-directory'
                    }

                    post {
                        always {
                            sh script: 'cat .build/tests.json | tparse -all -top -slow 100', returnStatus: true
                            archiveArtifacts artifacts: '.build/tests.json'
                            junit '.build/tests.xml'

                            script {
                                if(fileExists(".build/coverprofile")){
                                    sh script: 'filter-cover-profile < .build/coverprofile > .build/clean.coverprofile', returnStatus: true
                                    sh script: 'gocov convert .build/clean.coverprofile > .build/cover.json', returnStatus: true
                                    sh script: 'gocov-xml  < .build/cover.json > .build/cobertura.xml', returnStatus: true
                                    cobertura coberturaReportFile: '.build/cobertura.xml'
                                }
                            }
                        }
                    }
                }

                stage('Testsuite') {
                    environment {
                        STORJ_TEST_COCKROACH = 'cockroach://root@localhost:26257/testcockroach?sslmode=disable'
                        STORJ_TEST_POSTGRES = 'postgres://postgres@localhost/teststorj?sslmode=disable'
                        COVERFLAGS = "${ env.BRANCH_NAME != 'main' ? '' : '-coverprofile=.build/coverprofile -coverpkg=./...'}"
                    }
                    steps {
                        sh 'cockroach sql --insecure --host=localhost:26257 -e \'create database testcockroach;\''
                        sh 'psql -U postgres -c \'create database teststorj;\''
                        dir('testsuite'){
                            sh 'go vet ./...'
                            sh 'go test -parallel 4 -p 6 -vet=off $COVERFLAGS -timeout 20m -json -race ./... 2>&1 | tee ../.build/testsuite.json | xunit -out ../.build/testsuite.xml'
                        }
                        // TODO enable this later
                        // sh 'check-clean-directory'
                    }

                    post {
                        always {
                            sh script: 'cat .build/testsuite.json | tparse -all -top -slow 100', returnStatus: true
                            archiveArtifacts artifacts: '.build/testsuite.json'
                            junit '.build/testsuite.xml'
                        }
                    }
                }

                stage('Testsuite (against storj/storj master)') {
                    environment {
                        STORJ_TEST_COCKROACH = 'cockroach://root@localhost:26257/testcockroach_master?sslmode=disable'
                        STORJ_TEST_POSTGRES = 'postgres://postgres@localhost/teststorj_master?sslmode=disable'
                        COVERFLAGS = "${ env.BRANCH_NAME != 'master' ? '' : '-coverprofile=.build/coverprofile -coverpkg=./...'}"
                    }
                    steps {
                        sh 'cockroach sql --insecure --host=localhost:26257 -e \'create database testcockroach_master;\''
                        sh 'psql -U postgres -c \'create database teststorj_master;\''
                        dir('testsuite'){
                            sh 'go vet ./...'
                            sh 'go test --modfile go-master.mod -parallel 4 -p 6 -vet=off $COVERFLAGS -timeout 20m -json -race storj.io/uplink/testsuite 2>&1 | tee ../.build/testsuite.json | xunit -out ../.build/testsuite.xml'
                        }
                    }

                    post {
                        always {
                            sh script: 'cat .build/testsuite.json | tparse -all -top -slow 100', returnStatus: true
                            archiveArtifacts artifacts: '.build/testsuite.json'
                            junit '.build/testsuite.xml'
                        }
                    }
                }
                
                // TODO enable when storj/storj refactoring will be completed
                // stage('Integration [storj/storj]') {
                //     environment {
                //         STORJ_TEST_POSTGRES = 'postgres://postgres@localhost/teststorj2?sslmode=disable'
                //         STORJ_TEST_COCKROACH = 'omit'
                //     }
                //     steps {
                //         sh 'psql -U postgres -c \'create database teststorj2;\''
                //         dir('testsuite'){
                //             sh 'go vet storj.io/storj/...'
                //             sh 'go test -parallel 4 -p 6 -vet=off -timeout 20m -json storj.io/storj/... 2>&1 | tee ../.build/testsuite-storj.json | xunit -out ../.build/testsuite-storj.xml'
                //         }
                //     }

                //     post {
                //         always {
                //             sh script: 'cat .build/testsuite-storj.json | tparse -all -top -slow 100', returnStatus: true
                //             archiveArtifacts artifacts: '.build/testsuite-storj.json'
                //             junit '.build/testsuite-storj.xml'
                //         }
                //     }
                // }

                stage('Integration [tools]') {
                    environment {
                        STORJ_SIM_POSTGRES = 'postgres://postgres@localhost/teststorj3?sslmode=disable'
                    }
                    steps {
                        sh 'psql -U postgres -c \'create database teststorj3;\''
                        sh './testsuite/scripts/test-sim.sh'
                    }
                }
            }
        }
    }

    post {
        always {
            sh "chmod -R 777 ." // ensure Jenkins agent can delete the working directory
            deleteDir()
        }
    }
}
