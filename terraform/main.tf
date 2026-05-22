terraform {
  # Comentado para permitir uso local por quem clonar o repositório
  # backend "azurerm" {
  #   resource_group_name  = "tfstate-rg"
  #   storage_account_name = "tfstatemlops"
  #   container_name       = "tfstate"
  #   key                  = "mlops-pix.tfstate"
  # }
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "rg_mlops" {
  name     = "ProjetoMLOps-RG-v3"
  location = "Central US"
  tags = {
    Environment = "dev"
    Project     = "PIX-Fraud-MLOps"
  }
}

resource "azurerm_container_registry" "acr_mlops" {
  name                = "acrmlopspix"
  resource_group_name = azurerm_resource_group.rg_mlops.name
  location            = azurerm_resource_group.rg_mlops.location
  sku                 = "Basic"
  admin_enabled       = false
}

resource "azurerm_kubernetes_cluster" "aks_mlops" {
  name                = "ClusterMLOps"
  location            = azurerm_resource_group.rg_mlops.location
  resource_group_name = azurerm_resource_group.rg_mlops.name
  dns_prefix          = "clustermlops"
  kubernetes_version  = "1.28"  

  default_node_pool {
    name                 = "default"
    vm_size              = "Standard_D2s_v3"
    enable_auto_scaling  = true
    min_count            = 2
    max_count            = 4
  }

  identity {
    type = "SystemAssigned"
  }
} 

resource "azurerm_role_assignment" "aks_acr_pull" {
  principal_id                     = azurerm_kubernetes_cluster.aks_mlops.kubelet_identity[0].object_id
  role_definition_name             = "AcrPull"
  scope                            = azurerm_container_registry.acr_mlops.id
  skip_service_principal_aad_check = true
}