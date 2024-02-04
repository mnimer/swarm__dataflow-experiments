# To learn more about how to use Nix to configure your environment
# see: https://developers.google.com/idx/guides/customize-idx-env
{ pkgs, ... }: {
  channel = "stable-23.11"; # "stable-23.11" or "unstable"
  # Use https://search.nixos.org/packages to  find packages
  packages = [
    pkgs.jdk21
    pkgs.maven
    pkgs.google-cloud-sdk
#    pkgs.terraform
  ];
  # Sets environment variables in the workspace
  env = {};
  # search for the extension on https://open-vsx.org/ and use "publisher.id"
  idx.extensions = [
    "vscjava.vscode-java-pack"
    "googlecloudtools.cloudcode"
  ];
  # preview configuration, identical to monospace.json
  idx.previews = {
  };
  idx.workspace.onCreate = {
   
  };
}


