package auth

import (
	"context"
	"crypto/rsa"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type AuthConfig struct {
	PublicKeyPath string
	PublicKey     *rsa.PublicKey
	TLSConfigPath string
	TLSCfg        *TLSConfig
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

		token, err := parseJwtToken(tokenStr, pubKey)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
		}

		if err := validateToken(token); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func JwtMiddleware(pubKey *rsa.PublicKey) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing token"})
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		token, err := parseJwtToken(tokenStr, pubKey)
		if err != nil || !token.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}

		if claims, ok := token.Claims.(jwt.MapClaims); ok {
			c.Set("claims", claims)
		}

		c.Next()
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

func parseJwtToken(tokenStr string, pubKey *rsa.PublicKey) (*jwt.Token, error) {
	return jwt.Parse(tokenStr, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, ReturnUnauthenticatedError("unexpected signing method")
		}
		return pubKey, nil
	})
}

func validateToken(token *jwt.Token) error {
	if !token.Valid {
		return ReturnUnauthenticatedError("token is not valid")
	}

	// check expiry date
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return ReturnUnauthenticatedError("invalid token claims")
	}

	exp, ok := claims["exp"].(float64)
	if !ok || int64(exp) < time.Now().Unix() {
		return ReturnUnauthenticatedError("token has expired")
	}

	return nil
}
