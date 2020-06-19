airflow:
	docker-compose up --build

redshift-pause:
	aws redshift pause-cluster --cluster-identifier sparkify-dw --profile sparkify-dw-local --region us-west-2

redshift-resume:
	aws redshift resume-cluster --cluster-identifier sparkify-dw --profile sparkify-dw-local --region us-west-2