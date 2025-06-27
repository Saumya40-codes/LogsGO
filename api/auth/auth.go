package auth

import (
	"context"
	"crypto/rsa"
	"errors"
	"os"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v2"
)

type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type AuthConfig struct {
	PublicKeyPath string
	TLSConfigPath string
	Insecure      bool
}

func JwtInterceptor(pubKey *rsa.PublicKey) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, ReturnUnauthenticatedError("missing metadata")
		}

		authHeader := md["authorization"]
		if len(authHeader) == 0 {
			return nil, ReturnUnauthenticatedError("missing authorization header")
		}

		tokenStr := strings.TrimPrefix(authHeader[0], "Bearer ")
		if tokenStr == "" {
			return nil, ReturnUnauthenticatedError("missing token")
		}

		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, ReturnUnauthenticatedError("unexpected signing method")
			}
			return pubKey, nil
		})

		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
		}

		if !token.Valid {
			return nil, ReturnUnauthenticatedError("token is not valid")
		}

		return handler(ctx, req)
	}
}

func ReturnUnauthenticatedError(msg string) error {
	return status.Error(codes.Unauthenticated, msg)
}

func ParsePublicKeyFile(path string) (*rsa.PublicKey, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("failed to open public key file: " + err.Error())
	}
	defer file.Close()

	pubKeyData, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("failed to read public key file: " + err.Error())
	}

	pubKey, err := jwt.ParseRSAPublicKeyFromPEM(pubKeyData)
	if err != nil {
		return nil, errors.New("failed to parse public key: " + err.Error())
	}

	if pubKey == nil {
		return nil, errors.New("public key not found in file")
	}

	return pubKey, nil
}

func ParseTLSConfig(path string) (*TLSConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("failed to open TLS config file: " + err.Error())
	}
	defer file.Close()

	tlsConfig := &TLSConfig{}
	if err := yaml.NewDecoder(file).Decode(tlsConfig); err != nil {
		return nil, errors.New("failed to decode TLS config: " + err.Error())
	}
	if tlsConfig.CertFile == "" || tlsConfig.KeyFile == "" {
		return nil, errors.New("TLS config must contain cert_file and key_file")
	}
	return tlsConfig, nil
}

func GetTLSCredentials(tlsConfig *TLSConfig) (grpc.ServerOption, error) {
	if tlsConfig == nil {
		return nil, errors.New("TLS config is nil")
	}

	creds, err := credentials.NewServerTLSFromFile("certs/server.crt", "certs/server.key")
	if err != nil {
		return nil, errors.New("failed to create TLS credentials: " + err.Error())
	}

	return grpc.Creds(creds), nil
}
