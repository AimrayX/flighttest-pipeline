from airflow.sdk import dag, task

@dag(schedule=None, tags=["test"])
def hello():
    @task
    def say(msg: str) -> str:
        print(msg)
        return msg.upper()

    @task
    def shout(msg: str):
        print(msg)

    shout(say("hello from airflow"))

hello()