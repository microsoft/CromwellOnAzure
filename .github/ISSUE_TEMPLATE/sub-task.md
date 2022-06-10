---
name: Sub Task
about: A template for sub tasks to help break down large features or bugs
title: "[Issue # Quick Description] Sub work description "
labels: follow up
assignees: ''

---

Please customize title with related bug or feature number/quick description, as well as tag the issue number here with #[number] (no spaces).

This template is to help break down large features/bugs to help minimize massive PRs that could potentially cause conflicts. Please be sure to assign appropriate Project and Milestone that align with the parent issue.

An example of a large feature and some sub tasks for it: 
**Feature Request:** Create Managed MySQL on Azure # 63 
- **Sub Task**: [# 63 Azure MySQL]: Create flag in Configuration.cs # 64
- **Sub Task:** [# 63 Azure MySQL]: Deploy Azure MySQL Flexible Server before provisioning VM # 65
- **Sub Task:** [# 63 Azure MySQL]: Modify .conf file to point to Azure MySQL instead of local Docker image # 66
