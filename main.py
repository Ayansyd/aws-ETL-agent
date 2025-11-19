from agent.s3_agent import S3Agent
from config.aws_session import get_s3_client

def main():
    print("🚀 Modular S3 Agent")

    agent = S3Agent()

    while True:
        user_input = input("\n💬 ").strip()

        if user_input in ["exit", "quit"]:
            print("👋 Goodbye")
            break

        if user_input == "clear":
            agent.clear_history()
            continue

        agent.run(user_input)


if __name__ == "__main__":
    main()
