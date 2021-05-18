aws ecr create-repository --repository-name dq_alerts
docker build -t dq_alerts .
docker tag dq_alerts:latest 123456.dkr.ecr.eu-central-1.amazonaws.com/dq_alerts:latest
aws ecr get-login-password | docker login --username AWS --password-stdin 123456.dkr.ecr.eu-central-1.amazonaws.com
docker push 123456.dkr.ecr.eu-central-1.amazonaws.com/dq_alerts:latest