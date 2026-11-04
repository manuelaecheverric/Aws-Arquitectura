# Recursos creados por CONSOLA durante el taller (aprendizaje) y luego llevados a Terraform
# con bloques import (Terraform >= 1.5). La configuración se generó con
#   terraform plan -generate-config-out=console_resources.tf
# y se revisó para que el plan muestre "0 to change, 0 to destroy".

import {
  to = aws_s3_bucket.dw
  id = "dw-user3-ecommerce-20261027"
}
import {
  to = aws_s3_bucket_versioning.dw
  id = "dw-user3-ecommerce-20261027"
}
import {
  to = aws_s3_bucket_server_side_encryption_configuration.dw
  id = "dw-user3-ecommerce-20261027"
}
import {
  to = aws_s3_bucket_public_access_block.dw
  id = "dw-user3-ecommerce-20261027"
}
import {
  to = aws_s3_bucket_policy.dw
  id = "dw-user3-ecommerce-20261027"
}
import {
  to = aws_iam_role.redshift_s3
  id = "dw-user3-redshift-s3-role"
}
import {
  to = aws_iam_role_policy.redshift_s3
  id = "dw-user3-redshift-s3-role:dw-user3-leer-raw-s3"
}
import {
  to = aws_redshiftserverless_namespace.dw
  id = "dw-user3-namespace"
}
import {
  to = aws_redshiftserverless_workgroup.dw
  id = "dw-user3-workgroup"
}
