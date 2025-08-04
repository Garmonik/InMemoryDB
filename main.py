from database import InMemoryDB


def main():
    db = InMemoryDB()

    print("InMemory DataBase start working")
    print("Available commands: SET, GET, UNSET, COUNTS, FIND, BEGIN, ROLLBACK, COMMIT, END")

    command_handlers = {
        "END": lambda _: db.logger.info("SESSION ENDED") or exit(0),
        "SET": lambda args: db.set_value(args[0], args[1]) if len(args) == 2 else print("UNKNOWN COMMAND"),
        "GET": lambda args: print(db.get_value(args[0])) if len(args) == 1 else print("UNKNOWN COMMAND"),
        "UNSET": lambda args: db.unset_value(args[0]) if len(args) == 1 else print("UNKNOWN COMMAND"),
        "COUNTS": lambda args: print(db.count_values(args[0])) if len(args) == 1 else print("UNKNOWN COMMAND"),
        "FIND": lambda args: print(db.find_keys(args[0]) if isinstance(db.find_keys(args[0]), str) else " ".join(db.find_keys(args[0]))) if len(args) == 1 else print("UNKNOWN COMMAND"),
        "BEGIN": lambda _: db.begin_transaction(),
        "ROLLBACK": lambda _: print("NO TRANSACTION") if not db.rollback_transaction() else None,
        "COMMIT": lambda _: print("NO TRANSACTION") if not db.commit_transaction() else None,
    }

    while True:
        try:
            user_input = input("> ").split(" ")
            if not user_input:
                continue

            command = user_input[0].upper()
            args = user_input[1:]

            try:
                if command in command_handlers:
                    command_handlers[command](args)
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