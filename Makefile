lint:
	pylint --rcfile .pylintrc --load-plugins pylint_pydantic --extension-pkg-whitelist='pydantic' datapipe_image_moderation
	mypy --config-file setup.cfg datapipe_image_moderation
	black --check --config black.toml datapipe_image_moderation

format:
	black --verbose --config black.toml datapipe_image_moderation tests
	isort --sp .isort.cfg datapipe_image_moderation tests

test:
	pytest -s -vv