import os
import subprocess
import json
import time
import logging


class AWSECR:
    """
    A class to simplify AWS ECR operations with SSO authentication.
    """

    def __init__(self, profile_name: str = "", region: str = "", account_id=None):
        """
        Initialize the ECR manager with AWS profile and region.

        Parameters
        ----------
        profile_name: str
            AWS SSO profile name
        region: str
            AWS region
        account_id: str, optional
            AWS account ID. If None, will be retrieved from STS.
        """
        self.profile_name = profile_name
        self.region = region
        self.account_id = account_id
        self.logger = self._setup_logger()

        if not self._check_sso_session():
            self._login_sso()

    def _setup_logger(self):
        """Set up logging configuration."""
        logger = logging.getLogger("AWSECR")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _run_command(self, command, shell=False):
        """
        Run a shell command and return the output.

        Args:
            command (list or str): Command to run
            shell (bool): Whether to use shell execution

        Returns:
            str: Command output

        Raises:
            Exception: If command fails
        """
        try:
            self.logger.debug(f"Running command: {command}")
            result = subprocess.run(
                command, shell=shell, check=True, text=True, capture_output=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Command failed: {e}")
            self.logger.error(f"Error output: {e.stderr}")
            raise Exception(f"Command failed: {e}")

    def _check_sso_session(self):
        """
        Check if SSO session is active.

        Returns:
            bool: True if session is active, False otherwise
        """
        try:
            self._run_command(
                ["aws", "sts", "get-caller-identity", "--profile", self.profile_name]
            )
            return True
        except Exception:
            return False

    def _login_sso(self):
        """
        Start an AWS SSO session.

        Returns:
            bool: True if login successful, False otherwise
        """
        try:
            self.logger.info(f"Starting SSO login for profile {self.profile_name}...")
            self._run_command(["aws", "sso", "login", "--profile", self.profile_name])
            self.logger.info("SSO login successful")
            return True
        except Exception as e:
            self.logger.error(f"SSO login failed: {e}")
            return False

    def _get_account_id(self):
        """
        Get AWS account ID if not provided during initialization.

        Returns:
            str: AWS account ID
        """
        if not self.account_id:
            try:
                self.logger.info("Retrieving AWS account ID...")
                result = self._run_command(
                    [
                        "aws",
                        "sts",
                        "get-caller-identity",
                        "--profile",
                        self.profile_name,
                        "--output",
                        "json",
                    ]
                )
                self.account_id = json.loads(result)["Account"]
                self.logger.info(f"Account ID: {self.account_id}")
            except Exception as e:
                self.logger.error(f"Failed to get account ID: {e}")
                raise
        return self.account_id

    def _ensure_repository_exists(self, repo_name):
        """
        Create ECR repository if it doesn't exist.

        Args:
            repo_name (str): Repository name

        Returns:
            bool: True if repository exists or was created
        """
        try:
            self.logger.info(f"Checking if repository {repo_name} exists...")
            self._run_command(
                [
                    "aws",
                    "ecr",
                    "describe-repositories",
                    "--repository-names",
                    repo_name,
                    "--region",
                    self.region,
                    "--profile",
                    self.profile_name,
                ]
            )
            self.logger.info(f"Repository {repo_name} already exists")
            return True
        except Exception:
            try:
                self.logger.info(f"Creating repository {repo_name}...")
                self._run_command(
                    [
                        "aws",
                        "ecr",
                        "create-repository",
                        "--repository-name",
                        repo_name,
                        "--region",
                        self.region,
                        "--profile",
                        self.profile_name,
                    ]
                )
                self.logger.info(f"Repository {repo_name} created successfully")
                return True
            except Exception as e:
                self.logger.error(f"Failed to create repository: {e}")
                return False

    def _authenticate_docker_to_ecr(self):
        """
        Authenticate Docker to ECR.

        Returns:
            bool: True if authentication successful
        """
        try:
            self.logger.info("Authenticating Docker to ECR...")
            account_id = self._get_account_id()

            # Get ECR login token
            login_cmd = (
                f"aws ecr get-login-password --region {self.region} --profile {self.profile_name} | "
                f"docker login --username AWS --password-stdin {account_id}.dkr.ecr.{self.region}.amazonaws.com"
            )

            self._run_command(login_cmd, shell=True)
            self.logger.info("Docker authenticated to ECR successfully")
            return True
        except Exception as e:
            self.logger.error(f"Docker authentication failed: {e}")
            return False

    def _tag_and_push_image(self, local_image_name, repo_name, tag="latest"):
        """
        Tag and push a local Docker image to ECR.

        Args:
            local_image_name (str): Local image name
            repo_name (str): ECR repository name
            tag (str): Image tag

        Returns:
            bool: True if push successful
        """
        try:
            account_id = self._get_account_id()
            ecr_uri = (
                f"{account_id}.dkr.ecr.{self.region}.amazonaws.com/{repo_name}:{tag}"
            )

            # Tag the image
            self.logger.info(f"Tagging image {local_image_name} as {ecr_uri}...")
            self._run_command(["docker", "tag", f"{local_image_name}:{tag}", ecr_uri])

            # Push the image
            self.logger.info(f"Pushing image to {ecr_uri}...")
            self._run_command(["docker", "push", ecr_uri])

            self.logger.info("Image pushed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to tag and push image: {e}")
            return False

    def push_image_to_ecr(self, local_image_name, repo_name, tag="latest"):
        """
        Complete workflow to push an image to ECR with SSO authentication.

        Args:
            local_image_name (str): Local image name
            repo_name (str): ECR repository name
            tag (str): Image tag

        Returns:
            bool: True if all operations successful
        """
        # Check SSO session and login if needed
        if not self._check_sso_session():
            if not self._login_sso():
                return False

        # Ensure repository exists
        if not self._ensure_repository_exists(repo_name):
            return False

        # Authenticate Docker to ECR
        if not self._authenticate_docker_to_ecr():
            return False

        # Tag and push the image
        return self._tag_and_push_image(local_image_name, repo_name, tag)

    def _get_ecr_image_uri(self, repo_name, tag="latest"):
        """
        Get the full ECR URI for an image.

        Args:
            repo_name (str): ECR repository name
            tag (str): Image tag

        Returns:
            str: Full ECR URI for the image
        """
        account_id = self._get_account_id()
        return f"{account_id}.dkr.ecr.{self.region}.amazonaws.com/{repo_name}:{tag}"

    def pull_image_from_ec2(
        self, repo_name, tag="latest", ec2_host=None, ec2_key=None, ec2_user=None
    ):
        """
        Generate commands to pull an image from ECR on an EC2 instance.

        Args:
            repo_name (str): ECR repository name
            tag (str): Image tag
            ec2_host (str, optional): EC2 host address
            ec2_key (str, optional): Path to SSH key
            ec2_user (str, optional): EC2 user (default: ec2-user)

        Returns:
            str: Commands to run on EC2 instance
        """
        account_id = self._get_account_id()
        ecr_uri = self._get_ecr_image_uri(repo_name, tag)

        commands = [
            "# Run these commands on your EC2 instance:",
            f"aws ecr get-login-password --region {self.region} | docker login --username AWS --password-stdin {account_id}.dkr.ecr.{self.region}.amazonaws.com",
            f"docker pull {ecr_uri}",
        ]

        return "\n".join(commands)
