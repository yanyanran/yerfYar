package integration

import (
	"os"
	"os/exec"
	"testing"
)

// 确保etcd始终存在，因为它是测试的硬依赖项
func TestEtcdExists(t *testing.T) {
	etcdPath, err := exec.LookPath("etcd")
	if err != nil {
		t.Fatalf("在PATH中找不到Etcd")
	}

	cmd := exec.Command(etcdPath, "--help")
	cmd.Env = append(cmd.Env, os.Environ()...)
	cmd.Env = append(cmd.Env, "ETCD_UNSUPPORTED_ARCH=arm64")

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Errorf("运行`etcd--help`失败 (%v): %s", err, out)
	}
}
