from database import InMemoryDB


def main():
    db = InMemoryDB()

    print("InMemory DataBase start working")
    print("Available commands: SET, GET, UNSET, COUNTS, FIND, BEGIN, ROLLBACK, COMMIT, END")

    while True:
        try:
            user_input = input("> ").split(" ")
            if not user_input:
                continue

            command = user_input[0].upper()

            try:
                if command == "END":
                    db.logger.info("SESSION ENDED")
                    break
                elif command == "SET" and len(user_input) == 3:
                    db.set_value(user_input[1], user_input[2])
                elif command == "GET" and len(user_input) == 2:
                    print(db.get_value(user_input[1]))
                elif command == "UNSET" and len(user_input) == 2:
                    db.unset_value(user_input[1])
                elif command == "COUNTS" and len(user_input) == 2:
                    print(db.count_values(user_input[1]))
                elif command == "FIND" and len(user_input) == 2:
                    result = db.find_keys(user_input[1])
                    print(result if isinstance(result, str) else " ".join(result))
                elif command == "BEGIN":
                    db.begin_transaction()
                elif command == "ROLLBACK":
                    if not db.rollback_transaction():
                        print("NO TRANSACTION")
                elif command == "COMMIT":
                    if not db.commit_transaction():
                        print("NO TRANSACTION")
                else:
                    print("UNKNOWN COMMAND")
            except ValueError as e:
                print(f"ERROR: {e}")
                db.logger.error(f"Input error: {e}")
            except MemoryError as e:
                print(f"ERROR: {e}")
                db.logger.error(f"Error with memory: {e}")
            except RecursionError as e:
                print(f"ERROR: {e}")
                db.logger.error(f"Error with transaction: {e}")

        except EOFError:
            db.logger.info(f"Session terminated by EOF")
            print("\nSESSION ENDED")
            break
        except KeyboardInterrupt:
            db.logger.info(f"Session terminated by user")
            print("\nSESSION INTERRUPTED")
            break
        except Exception as e:
            print(f"CRITICAL ERROR: {str(e)}")
            db.logger.critical(f"Unspecified error: {str(e)}")
            break


if __name__ == "__main__":
    main()