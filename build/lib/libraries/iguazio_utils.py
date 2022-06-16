def create_func(mlrun, **kwargs):
    mlrun.set_environment(project='rxp-simulation-abhi')
    project = mlrun.get_or_create_project('rxp-simulation-abhi', context='./')
    sj = mlrun.new_function(kind='spark',
                            project='rxp-simulation-abhi',
                            command='/v3io/new-rxp/runner.py',
                            name='new_rxp_job',
                            image='.mlrun/func-{}-{}:{}'.format('rxp-simulation-abhi',
                                                                'rxp_job_simulation',
                                                                'latest')
                            )

    sj.with_driver_limits(cpu='1200m')
    sj.with_driver_requests(cpu=1,
                            mem='512m')
    sj.with_executor_limits(cpu='1200m')
    sj.with_executor_requests(cpu=1,
                              mem='512m')
    sj.with_igz_spark()
    sj.spec.deps['pyFiles'] = ['local:///v3io/new-rxp/rx_perso_spark_iguazio-1.0-py3.8.egg']
    sj.spec.deps['packages'] = ['net.snowflake:snowflake-jdbc:3.13.14', 'net.snowflake:spark-snowflake_2.12:2.9.3-spark_3.0']
    sj.spec.replicas = 1
    sj.apply(mlrun.mount_v3io())
    sj.deploy()


def run_func(mlrun, **kwargs):

    mlrun.set_environment(project='rxp-simulation-abhi')
    project = mlrun.get_or_create_project('rxp-simulation-abhi', context="./")
    trig_func = project.get_function('new_rxp_job')
    trig_func.spec.args = ['-m','{}.{}.{}'.format('spark','testRun','jobA'),
                           '-e', '{}'.format('dev'),
                           '-r', '{}'.format('run_id'),
                           '-c', '{}'.format('/v3io/new-rxp/configs'),
                           '-p', '{}'.format('path_to_spark_packages'),
                           '-pj', '{}'.format('project_file_name'),
                           '-ss', '{}'.format('spark_settings'),
                           '-d', '{}'.format('dry_run_flag')]
    trig_func.run()

if __name__ == '__main__':

    import os
    os.environ['MLRUN_DBPATH'] = 'https://mlrun-api.default-tenant.app.cvsrx.iguazio-c0.com'
    os.environ['MLRUN_ARTIFACT_PATH'] = '/User/artifacts/{{run.project}}'
    os.environ['V3IO_USERNAME'] = 'abhishek'
    os.environ['V3IO_API'] = 'https://webapi.default-tenant.app.cvsrx.iguazio-c0.com:8444/'
    os.environ['V3IO_ACCESS_KEY'] = ''
    import mlrun
    # create_func(mlrun)
    run_func(mlrun)
