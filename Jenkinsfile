pipeline {
    agent none

    options {
          timeout(time: 80, unit: 'MINUTES')
    }
    environment {
        NPM_CONFIG_CACHE = '/tmp/npm/cache'
        COCKROACH_MEMPROF_INTERVAL=0
    }
    stages {
        stage('Build') {
            agent {
                docker {
                    label 'main'
                    image 'storjlabs/ci:latest'
                    alwaysPull true
                    args '-u root:root --cap-add SYS_PTRACE' +
                        ' -v "/cache/gomod":/go/pkg/mod' +
                        ' -v /cache/golangci-lint:/root/.cache/golangci-lint' +
                        ' -v /cache/gobuild:/root/.cache/go-build' +
                        ' -v /cache/gobenchmarks:/root/.cache/go-build-benchmarks' +
                        ' -v /cache/gomain:/root/.cache/go-build-main' +
                        ' -v /cache/uplinkintegration:/root/.cache/go-build-integration' +
                        ' -v "/cache/npm":/npm'
                }
            }
            stages {
                stage('Preparation') {
                    parallel {
                        stage('Checkout') {
                            steps {
                                checkout scm
                                sh 'git restore-mtime'

                                sh 'mkdir -p .build'
                                // make a backup of the mod file in case, for later linting
                                sh 'cp go.mod .build/go.mod.orig'
                                sh 'cp testsuite/go.mod .build/testsuite.go.mod.orig'
                            }
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
                                sh 'GOOS=linux GOARCH=arm staticcheck ./...'
                                sh 'golangci-lint --config /go/ci/.golangci.yml -j=2 run'
                                sh 'go-licenses check ./...'
                                sh './scripts/check-libuplink-size.sh'
                                sh 'check-mod-tidy -mod .build/go.mod.orig'

                                dir("testsuite") {
                                    sh 'check-imports ./...'
                                    sh 'check-atomic-align ./...'
                                    sh 'check-monkit ./...'
                                    sh 'check-errs ./...'
                                    sh 'staticcheck ./...'
                                    sh 'golangci-lint --config /go/ci/.golangci.yml -j=2 run'
                                    sh 'check-mod-tidy -mod ../.build/testsuite.go.mod.orig'
                                }
                            }
                        }

                        stage('Tests') {
                            environment {
                                COVERFLAGS = "${ env.BRANCH_NAME == 'main' ? '-coverprofile=.build/coverprofile -coverpkg=./...' : ''}"
                            }
                            steps {
                                sh 'go vet ./...'
                                sh 'go test -parallel 6 -p 12 -vet=off $COVERFLAGS -timeout 40m -json -race ./... 2>&1 | tee .build/tests.json | xunit -out .build/tests.xml'
                            }

                            post {
                                always {
                                    sh script: 'cat .build/tests.json | tparse -all -slow 100', returnStatus: true
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
                                STORJ_TEST_COCKROACH = 'omit'
                                STORJ_TEST_POSTGRES = 'omit'
                                STORJ_TEST_LOG_LEVEL = 'info'
                                STORJ_HASHSTORE_TABLE_DEFAULT_KIND = 'memtbl'
                                COVERFLAGS = "${ env.BRANCH_NAME == 'main' ? '-coverprofile=../.build/testsuite_coverprofile -coverpkg=storj.io/uplink/...' : ''}"
                                STORJ_TEST_SPANNER = 'run:/usr/local/bin/spanner_emulator --override_change_stream_partition_token_alive_seconds=1'
                                SPANNER_DISABLE_BUILTIN_METRICS = 'true'
                                GOOGLE_CLOUD_SPANNER_DISABLE_LOG_CLIENT_OPTIONS='true'
                            }
                            steps {
                                dir('testsuite'){
                                    sh 'go vet ./...'
                                    sh 'go test -parallel 8 -p 12 -vet=off $COVERFLAGS -timeout 40m -json -race ./... 2>&1 | tee ../.build/testsuite.json | xunit -out ../.build/testsuite.xml'
                                }
                            }

                            post {
                                always {
                                    sh script: 'cat .build/testsuite.json | tparse -all -slow 100', returnStatus: true
                                    archiveArtifacts artifacts: '.build/testsuite.json'
                                    junit '.build/testsuite.xml'

                                    script {
                                        if(fileExists(".build/testsuite_coverprofile")){
                                            sh script: 'filter-cover-profile < .build/testsuite_coverprofile > .build/clean.testsuite_coverprofile', returnStatus: true
                                            sh script: 'gocov convert .build/clean.testsuite_coverprofile > .build/testsuite_cover.json', returnStatus: true
                                            sh script: 'gocov-xml  < .build/testsuite_cover.json > .build/testsuite_cobertura.xml', returnStatus: true
                                            cobertura coberturaReportFile: '.build/testsuite_cobertura.xml'
                                        }
                                    }
                                }
                            }
                        }

                        stage('Go Compatibility') {
                            steps {
                                sh 'check-cross-compile -compiler "go,go.min" storj.io/uplink/...'
                            }
                        }
                    }
                }

                stage('Integration storj/storj using Spanner') {
                    environment {
                        STORJ_TEST_POSTGRES = 'omit'
                        STORJ_TEST_COCKROACH = 'omit'
                        STORJ_TEST_SPANNER = 'run:/usr/local/bin/spanner_emulator --override_change_stream_partition_token_alive_seconds=1'
                        GOCACHE = '/root/.cache/go-build-integration'
                    }
                    steps {
                        dir('testsuite'){
                            sh 'cp go.mod go-temp.mod'

                            sh 'go vet -modfile go-temp.mod -mod=mod storj.io/storj/...'
                            sh 'go test -modfile go-temp.mod -mod=mod -tags noembed -parallel 8 -p 12 -vet=off -timeout 80m -json $(go list storj.io/storj/... | grep -v /metabase | grep -v /satellitedb) 2>&1 | tee ../.build/testsuite-storj.json | xunit -out ../.build/testsuite-storj.xml'
                        }
                    }

                    post {
                        always {
                            sh script: 'cat .build/testsuite-storj.json | tparse -all -slow 100', returnStatus: true
                            archiveArtifacts artifacts: '.build/testsuite-storj.json'
                            junit '.build/testsuite-storj.xml'
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

        stage('Integration [rclone]') {
            agent {
                node {
                    label 'ondemand'
                }
            }
            steps {
                    echo 'Testing rclone integration'
                    sh './testsuite/scripts/rclone.sh'
            }
            post {
                always {
                    zip zipFile: 'rclone-integration-tests.zip', archive: true, dir: '.build/rclone-integration-tests'
                    sh "chmod -R 777 ." // ensure Jenkins agent can delete the working directory
                    deleteDir()
                }
            }
        }
    }
}
