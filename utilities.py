from pathlib import Path


from terminal_colors import TerminalColors as tc


class Utilities:
    # propert to get the relative path of shared files
    @property
    def shared_files_path(self) -> Path:
        """Get the path to the shared files directory."""
        return Path(__file__).parent.resolve() / "shared"

    def load_instructions(self, instructions_file: str) -> str:
        """Load instructions from a file."""
        file_path = self.shared_files_path / instructions_file
        with file_path.open("r", encoding="utf-8", errors="ignore") as file:
            return file.read()

    def log_msg_green(self, msg: str) -> None:
        """Print a message in green."""
        print(f"{tc.GREEN}{msg}{tc.RESET}")

    def log_msg_purple(self, msg: str) -> None:
        """Print a message in purple."""
        print(f"{tc.PURPLE}{msg}{tc.RESET}")

    def log_token_blue(self, msg: str) -> None:
        """Print a token in blue."""
        print(f"{tc.BLUE}{msg}{tc.RESET}", end="", flush=True)

   
